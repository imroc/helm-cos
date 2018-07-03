package cmd

import (
	"github.com/imroc/helm-cos/pkg/repo"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init cos://bucket/path",
	Short: "init a repository",
	Long:  `This command will initialize a new repository on a given COS url (cos://bucket/path).`,
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		r, err := repo.New(args[0])
		if err != nil {
			return err
		}
		return repo.Create(r)
	},
}

func init() {
	RootCmd.AddCommand(initCmd)
}
