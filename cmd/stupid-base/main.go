/*
Copyright (c) 2020, pigeonligh.
*/

package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/c-bata/go-prompt"

	"github.com/pigeonligh/stupid-base/pkg/core/database"
)

func main() {
	db, _ = database.New()

	switch len(os.Args) {
	case 1:
		fmt.Println("  _________ __               .__    .___ __________                       ")
		fmt.Println(" /   _____//  |_ __ ________ |__| __| _/ \\______   \\_____    ______ ____  ")
		fmt.Println(" \\_____  \\\\   __\\  |  \\____ \\|  |/ __ |   |    |  _/\\__  \\  /  ___// __ \\ ")
		fmt.Println(" /        \\|  | |  |  /  |_> >  / /_/ |   |    |   \\ / __ \\_\\___ \\\\  ___/ ")
		fmt.Println("/_______  /|__| |____/|   __/|__\\____ |   |______  /(____  /____  >\\___  >")
		fmt.Println("        \\/            |__|           \\/          \\/      \\/     \\/     \\/ ")
		fmt.Println("")

		reader := prompt.New(executor, completer,
			prompt.OptionTitle("sql-prompt"),
			prompt.OptionHistory(history),
			prompt.OptionPrefixTextColor(prompt.Yellow),
			prompt.OptionPreviewSuggestionTextColor(prompt.Blue),
			prompt.OptionSelectedSuggestionBGColor(prompt.LightGray),
			prompt.OptionSuggestionBGColor(prompt.DarkGray),
			prompt.OptionPrefix("stupid-base >>> "),
			prompt.OptionLivePrefix(changeLivePrefix),
		)

		reader.Run()

	case 2:
		sqls, err := ioutil.ReadFile(os.Args[1])
		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}

		err = db.Run(string(sqls))
		if err != nil {
			fmt.Println("Error:", err)
		}

	default:
		fmt.Println("unknown parameters")
		os.Exit(1)
	}
}
