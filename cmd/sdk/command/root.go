package command

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var rootCMD = &cobra.Command{
	Use:   "rstorage ctl",
	Short: "Copyright © 2022 RSTORAGE: a commands tools for rstorage cluster",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("./rstorage-ctl -h for help")
			return
		}
	},
}

func init() {}

func Execute() {
	if err := rootCMD.Execute(); err != nil {
		os.Exit(-1)
	}
}
