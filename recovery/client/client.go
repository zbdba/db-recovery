// Copyright 2019 The zbdba Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"os"

	"github.com/spf13/cobra"
)


const (
	cliName        = "github.com/zbdba/db-recovery"
	cliDescription = "A simple command line client for github.com/zbdba/db-recovery."
)

var rootCmd *cobra.Command

func init() {
	Desc := PrintLogo() + cliDescription
	rootCmd = NewRootCommand(cliName, Desc)
}

func Main() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
