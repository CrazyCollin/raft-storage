package command

import (
	"fmt"
	"github.com/spf13/cobra"
	"rstorage/pkg/metaserver"
	pb "rstorage/pkg/protocol"
	"strings"
)

var getMetaConfigCMD = &cobra.Command{
	Use:   "get-config [meta-server addresses]",
	Short: "get meta server config",
	Run: func(cmd *cobra.Command, args []string) {
		metaServerAddresses := strings.Split(args[0], ",")

		metaServerClient := metaserver.NewMetaServiceCli(metaServerAddresses)

		request := &pb.ServerGroupMetaConfigRequest{
			ConfigVersion: -1,
			OpType:        pb.ConfigServerGroupMetaOpType_OP_SERVER_GROUP_QUERY,
		}

		response := metaServerClient.GetServerGroupMeta(request)

		fmt.Println(response)

	},
}

func init() {
	rootCMD.AddCommand(getMetaConfigCMD)
}
