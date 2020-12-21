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

var (
	ProjectName     = "db-recovery"
	Major           = 1
	Minor           = 0
	Patch           = 0
	GitSHA          = "Not provided"
	BuildTime       = "Not privided"
)

func PrintLogo() string{
	// http://patorjk.com/software/taag/#p=display&f=Slant&t=db-recovery
	LogoStr := `
       ____                                                   
  ____/ / /_        ________  _________ _   _____  _______  __
 / __  / __ \______/ ___/ _ \/ ___/ __ \ | / / _ \/ ___/ / / /
/ /_/ / /_/ /_____/ /  /  __/ /__/ /_/ / |/ /  __/ /  / /_/ / 
\__,_/_.___/     /_/   \___/\___/\____/|___/\___/_/   \__, /  
                                                     /____/
`
	return LogoStr
}