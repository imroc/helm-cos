# helm-cos [![Build Status](https://travis-ci.org/nouney/helm-cos.svg?branch=master)](https://travis-ci.org/nouney/helm-cos)

`helm-cos` is a [helm](https://github.com/kubernetes/helm) plugin that allows you to manage private helm repositories on Tencent Cloud Object Storage.

## Installation

Install the latest version:
```shell
$ helm plugin install https://github.com/imroc/helm-cos
```

Install a specific version:
```shell
$ helm plugin install https://github.com/imroc/helm-cos --version 0.2.0
```

## Quick start

```shell
# Init a new repository
$ helm cos init cos://bucket/path

# Add your repository to Helm
$ helm repo add repo-name cos://bucket/path

# Push a chart to your repository
$ helm cos push chart.tar.gz repo-name

# Update Helm cache
$ helm repo update

# Fetch the chart
$ helm fetch repo-name/chart

# Remove the chart
$ helm cos rm chart repo-name
```

## Documentation

### Authentification

TODO


### Create a repository

First, you need to [create a bucket on COS](https://cloud.tencent.com/document/product/436/6232), which will be used by the plugin to store your charts.

Then you have to initialize a repository at a specific location in your bucket:

```shell
$ helm cos init cos://your-bucket/path
```

>   You can create a repository anywhere in your bucket.

>   This command does nothing if a repository already exists at the given location.

You can now add the repository to helm:
```shell
$ helm repo add my-repository cos://your-bucket/path
```

### Push a chart

Package the chart:
```shell
$ helm package my-chart
```
This will create a file `my-chart-<semver>.tgz`.

Now, to push the chart to the repository `my-repository`:

```shell
$ helm cos push my-chart-<semver>.tgz my-repository
```

If you got this error:
```shell
Error: update index file: index is out-of-date
```

That means that someone/something updated the same repository, at the same time as you. You just need to execute the command again or, next time, use the `--retry` flag to automatically retry to push the chart.

Once the chart is uploaded, use helm to fetch it:

```shell
# Update local repo cache if necessary
# $ helm repo update

$ helm fetch my-chart
```

>   This command does nothing if the same chart (name and version) already exists.

>   Using `--retry` is highly recommended in a CI/CD environment.

### Remove a chart

You can remove all the versions of a chart from a repository by running:

```shell
$ helm cos remove my-chart my-repository
```

To remove a specific version, simply use the `--version` flag:

```shell
$ helm cos remove my-chart my-repository --version 0.1.0
```

>   Don't forget to run `helm repo up` after you remove a chart.

## Troubleshooting

You can use the global flag `--debug` to get more informations. Please write an issue if you find any bug.