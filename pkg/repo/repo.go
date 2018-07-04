package repo

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/helm/pkg/chartutil"
	"k8s.io/helm/pkg/helm/environment"
	"k8s.io/helm/pkg/helm/helmpath"
	"k8s.io/helm/pkg/proto/hapi/chart"
	"k8s.io/helm/pkg/provenance"
	"k8s.io/helm/pkg/repo"

	"github.com/imroc/helm-cos/cmd/conf"
	"github.com/imroc/helm-cos/pkg/cos"
	"net/http"
)

var (
	// ErrIndexOutOfDate occurs when trying to push a chart on a repository
	// that is being updated at the same time.
	ErrIndexOutOfDate = errors.New("index is out-of-date")

	// Debug is used to activate log output
	Debug bool
)

// Repo manages Helm repositories on Google Cloud Storage.
type Repo struct {
	entry    *repo.Entry
	basePath string
	//indexFileGeneration int64
	cos *cos.Client
}

func (r *Repo) getIndexFileURL() string {
	return "gs://" + path.Join(r.cos.GetEndpoint(""), r.basePath, "index.yaml")
}

func (r *Repo) checkExsits(file string) (bool, error) {
	bkt := r.cos.Bucket("")
	resp, err := bkt.Head(path.Join(r.basePath, file), make(http.Header))
	if err != nil {
		return false, errors.WithStack(err)
	}
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("unknown status: %s", resp.Status)
}

// New creates a new Repo object
func New(path string) (*Repo, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	client, err := conf.GetCosClient(u.Host)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	basePath := u.Path
	if basePath == "" {
		basePath = "/"
	}
	return &Repo{
		basePath: basePath,
		entry:    nil,
		cos:      client,
	}, nil
}

// Load loads an existing repository known by Helm.
// Returns ErrNotFound if the repository is not found in helm repository entries.
func Load(name string) (*Repo, error) {
	entry, err := retrieveRepositoryEntry(name)
	if err != nil {
		return nil, errors.Wrap(err, "entry")
	}
	if entry == nil {
		return nil, fmt.Errorf("repository \"%s\" not found. Make sure you add it to helm", name)
	}

	u, err := url.Parse(entry.URL)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cos, err := conf.GetCosClient(u.Host)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &Repo{
		entry:    entry,
		basePath: u.Path,
		cos:      cos,
	}, nil
}

// Create creates a new repository on GCS.
// This function is idempotent.
func Create(r *Repo) error {
	log := logger()
	log.Debugf("create a repository with index file at %s", r.getIndexFileURL())

	exsits, err := r.checkExsits("index.yaml")
	if err != nil {
		return errors.WithStack(err)
	}
	if exsits {
		log.Debugf("file %s already exists", r.getIndexFileURL())
		return nil
	}
	i := repo.NewIndexFile()
	return r.uploadIndexFile(i)
}

// PushChart adds a chart into the repository.
//
// The index file on GCS will be updated and the file at "chartpath" will be uploaded to GCS.
// If the version of the chart is already indexed, it won't be uploaded unless "force" is set to true.
// The push will fail if the repository is updated at the same time, use "retry" to automatically reload
// the index of the repository.
func (r *Repo) PushChart(chartpath string, force bool) error {
	log := logger()
	i, err := r.indexFile()
	if err != nil {
		return errors.Wrap(err, "load index file")
	}

	log.Debugf("load chart \"%s\" (force=%t)", chartpath, force)
	chart, err := chartutil.Load(chartpath)
	if err != nil {
		return errors.Wrap(err, "load chart")
	}

	log.Debugf("chart loaded: %s-%s", chart.Metadata.Name, chart.Metadata.Version)
	if i.Has(chart.Metadata.Name, chart.Metadata.Version) && !force {
		return fmt.Errorf("chart %s-%s already indexed. Use --force to still upload the chart", chart.Metadata.Name, chart.Metadata.Version)
	}

	if !i.Has(chart.Metadata.Name, chart.Metadata.Version) {
		err := r.updateIndexFile(i, chartpath, chart)
		if err != nil {
			return errors.Wrap(err, "update index file")
		}
	}

	log.Debugf("upload file to COS")
	err = r.uploadChart(chartpath)
	if err != nil {
		return errors.Wrap(err, "write chart")
	}
	return nil
}

// RemoveChart removes a chart from the repository
// If version is empty, all version will be deleted.
func (r *Repo) RemoveChart(name, version string) error {
	log := logger()
	log.Debugf("removing chart %s-%s", name, version)

	index, err := r.indexFile()
	if err != nil {
		return errors.Wrap(err, "index")
	}

	vs, ok := index.Entries[name]
	if !ok {
		return fmt.Errorf("chart \"%s\" not found", name)
	}

	urls := []string{}
	for i, v := range vs {
		if version == "" || version == v.Version {
			log.Debugf("%s-%s will be deleted", name, v.Version)
			urls = append(urls, v.URLs...)
		}
		if version == v.Version {
			index.Entries[name] = append(vs[:i], vs[i+1:]...)
			break
		}
	}
	if version == "" || len(index.Entries[name]) == 0 {
		delete(index.Entries, name)
	}

	err = r.uploadIndexFile(index)
	if err != nil {
		return err
	}

	bkt := r.cos.Bucket("")
	// Delete charts from COS
	for _, rawurl := range urls {
		u, err := url.Parse(rawurl)
		if err != nil {
			log.Errorf("bad url:%s", rawurl)
			continue
		}
		log.Debugf("delete cos file %s", rawurl)
		err = bkt.Del(u.Host)
		if err != nil {
			log.Errorf("failed to remove chart:%s", rawurl)
			continue
		}
	}
	return nil
}

const DefaultContentType = "application/octet-stream"

// uploadIndexFile update the index file on GCS.
func (r *Repo) uploadIndexFile(i *repo.IndexFile) error {
	log := logger()
	log.Debugf("push index file")
	i.SortEntries()

	b, err := yaml.Marshal(i)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	bkt := r.cos.Bucket("")
	err = bkt.Put(path.Join(r.basePath, "index.yaml"), b, DefaultContentType, cos.Private, cos.Options{})
	if err != nil {
		return errors.Wrap(err, "write")
	}
	return nil
}

// indexFile retrieves the index file from GCS.
// It will also retrieve the generation number of the file, for optimistic locking.
func (r *Repo) indexFile() (*repo.IndexFile, error) {
	log := logger()
	log.Debugf("load index file \"%s\"", r.getIndexFileURL())

	// retrieve index file generation
	bkt := r.cos.Bucket("")
	b, err := bkt.Get(path.Join(r.basePath, "index.yaml"))
	if err != nil {
		return nil, errors.Wrap(err, "get index.yaml")
	}

	i := &repo.IndexFile{}
	if err := yaml.Unmarshal(b, i); err != nil {
		return nil, errors.Wrap(err, "unmarshal")
	}
	i.SortEntries()
	return i, nil
}

// uploadChart pushes a chart into the repository.
func (r *Repo) uploadChart(chartpath string) error {
	log := logger()
	f, err := os.Open(chartpath)
	if err != nil {
		return errors.Wrap(err, "open")
	}
	_, fname := filepath.Split(chartpath)
	path := path.Join(r.basePath, fname)
	log.Debugf("upload file %s to cos path %s", fname, path)
	bkt := r.cos.Bucket("")
	state, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "file state")
	}
	err = bkt.PutReader(path, f, state.Size(), DefaultContentType, cos.Private, cos.Options{})
	if err != nil {
		return errors.Wrap(err, "upload chart file")
	}
	return nil
}

func (r Repo) updateIndexFile(i *repo.IndexFile, chartpath string, chart *chart.Chart) error {
	log := logger()
	hash, err := provenance.DigestFile(chartpath)
	if err != nil {
		return errors.Wrap(err, "digest file")
	}
	_, fname := filepath.Split(chartpath)
	log.Debugf("indexing chart '%s-%s' as '%s' (base url: %s)", chart.Metadata.Name, chart.Metadata.Version, fname, r.entry.URL)
	i.Add(chart.GetMetadata(), fname, r.entry.URL, hash)
	return r.uploadIndexFile(i)
}

func retrieveRepositoryEntry(name string) (*repo.Entry, error) {
	log := logger()
	helmHome := os.Getenv("HELM_HOME")
	if helmHome == "" {
		helmHome = environment.DefaultHelmHome
	}
	log.Debugf("helm home: %s", helmHome)
	h := helmpath.Home(helmHome)
	repoFile, err := repo.LoadRepositoriesFile(h.RepositoryFile())
	if err != nil {
		return nil, errors.Wrap(err, "load")
	}
	for _, r := range repoFile.Repositories {
		if r.Name == name {
			return r, nil
		}
	}
	return nil, errors.Wrapf(err, "repository \"%s\" does not exist", name)
}

func logger() *logrus.Entry {
	l := logrus.New()
	level := logrus.InfoLevel
	if Debug || strings.ToLower(os.Getenv("HELM_GCS_DEBUG")) == "true" {
		level = logrus.DebugLevel
	}
	l.SetLevel(level)
	l.Formatter = &logrus.TextFormatter{}
	return logrus.NewEntry(l)
}
