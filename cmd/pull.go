// Copyright © 2018 Valentin Tjoncke <valtjo@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"io"
	"os"

	"github.com/imroc/helm-cos/cmd/conf"
	"github.com/spf13/cobra"
	"net/url"
)

var pullCmd = &cobra.Command{
	Use:   "pull cos://bucket/path",
	Short: "prints a file on stdout",
	Long: `This command pull a file from COS and prints it to stdout.
Used by helm to fetch charts from COS.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		u, err := url.Parse(args[0])
		if err != nil {
			return err
		}
		client, err := conf.GetCosClient(u.Host)
		if err != nil {
			return err
		}
		bkt := client.Bucket("")
		rc, err := bkt.GetReader(u.Path)
		if err != nil {
			return err
		}
		_, err = io.Copy(os.Stdout, rc)
		return err
	},
}

func init() {
	RootCmd.AddCommand(pullCmd)
}
