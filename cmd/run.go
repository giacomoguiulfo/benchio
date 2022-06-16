// Copyright © 2018 Giacomo Guiulfo <giacomoguiulfo@gmail.com>
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
	"github.com/giacomoguiulfo/benchio/pkg/bench"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run benchmark tests",
	Long:  ``,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		var conf = &bench.Config{
			AccessKey:        viper.GetString("accessKey"),
			SecretKey:        viper.GetString("secretKey"),
			Endpoint:         viper.GetString("endpoint"),
			Bucket:           viper.GetString("bucket"),
			MultipartSize:    viper.GetInt64("multipartSize"),
			ObjectSize:       viper.GetInt64("objectSize"),
			ObjectSplit:      viper.GetSizeInBytes("objectSplit"),
			ObjectNamePrefix: viper.GetString("objectNamePrefix"),
			Clients:          viper.GetSizeInBytes("numClients"),
			ObjectCount:      viper.GetSizeInBytes("numSamples"),
			Verbose:          viper.GetBool("verbose"),
			Region:           viper.GetString("region"),
			Write:            viper.GetBool("write"),
			Read:             viper.GetBool("read"),
			Cleanup:          viper.GetBool("cleanup"),
		}
		bench.Mark(conf)
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().String("endpoint", "", "AWS endpoint (required)")
	viper.BindPFlag("endpoint", runCmd.Flags().Lookup("endpoint"))
	runCmd.Flags().Bool("cleanup", true, "cleanup objects after testing")
	viper.BindPFlag("cleanup", runCmd.Flags().Lookup("cleanup"))
	runCmd.Flags().BoolP("write", "w", true, "perform write tests")
	viper.BindPFlag("write", runCmd.Flags().Lookup("write"))
	runCmd.Flags().BoolP("read", "r", true, "perform read tests")
	viper.BindPFlag("read", runCmd.Flags().Lookup("read"))
}
