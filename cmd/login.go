package cmd

import (
	"github.com/imroc/helm-cos/cmd/conf"
	"github.com/spf13/cobra"
	"net/url"
)

var (
	secretId, secretKey string
)

var loginCmd = &cobra.Command{
	Use:   "login cos://bucket/path",
	Short: "login a repository",
	Long:  `This command will login a repository on a given COS url (cos://bucket/path).`,
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		u, err := url.Parse(args[0])
		if err != nil {
			return err
		}
		err = conf.UpdateBucketConfig(u.Host, &conf.CosConfig{
			SecretId:  secretId,
			SecretKey: secretKey,
		})
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	RootCmd.AddCommand(loginCmd)
	loginCmd.Flags().StringVar(&secretId, "secretid", "", "COS SecretId")
	loginCmd.Flags().StringVar(&secretKey, "secretkey", "", "COS SecretKey")
}
