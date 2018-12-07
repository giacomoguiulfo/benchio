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
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type createCfg struct {
	objectSize       int64
	objectCount      uint
	objectNamePrefix string
	verbose          bool
	directory        string
}

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Creates benchio's workload locally",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		createWorkload(&createCfg{
			objectSize:       viper.GetInt64("objectSize"),
			objectNamePrefix: viper.GetString("objectNamePrefix"),
			objectCount:      viper.GetSizeInBytes("numSamples"),
			verbose:          viper.GetBool("verbose"),
			directory:        viper.GetString("directory"),
		})
	},
}

func init() {
	rootCmd.AddCommand(createCmd)

	createCmd.Flags().StringP("directory", "d", ".", "Directory to save the workload")
	viper.BindPFlag("directory", createCmd.Flags().Lookup("directory"))
}

func createWorkload(cfg *createCfg) {
	if cfg.verbose == true {
		fmt.Println("Generating in-memory sample data... ")
	}
	timeGenData := time.Now()
	buffer := make([]byte, cfg.objectSize, cfg.objectSize)
	_, err := rand.Read(buffer)
	if err != nil {
		fmt.Println("Could not allocate a buffer")
		os.Exit(1)
	}
	for i := uint(0); i < cfg.objectCount; i++ {
		basename := fmt.Sprintf("%s%d", cfg.objectNamePrefix, i)
		absname := filepath.Join(cfg.directory, basename)
		if cfg.verbose == true {
			fmt.Printf("Wrote %d bytes to %s\n", cfg.objectSize, absname)
		}
		err := ioutil.WriteFile(absname, buffer, 0755)
		if err != nil {
			fmt.Printf("Unable to write file: %v\n", err)
			os.Exit(1)
		}
	}
	if cfg.verbose == true {
		fmt.Printf("Done (%s)\n", time.Since(timeGenData))
	}
}
