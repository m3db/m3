package client

import (
	"github.com/m3db/m3/src/dbnode/auth"
	"github.com/m3db/m3/src/dbnode/environment"
)

// PopulateClientOutboundAuthConfig takes clients dynamic config as populate auth global registry.
func PopulateClientOutboundAuthConfig(cfg environment.DynamicConfiguration) error {
	var dbnodeCredentials []auth.OutboundCredentials
	var etcdCredentails []auth.OutboundCredentials

	for _, clusterCfg := range cfg {
		if ok := clusterCfg.Service.Auth.Enabled; ok {
			if err := clusterCfg.Service.Auth.Validate(); err != nil {
				return err
			}
			nodeCredential := auth.OutboundCredentials{
				Username: clusterCfg.Service.Auth.UserName,
				Password: clusterCfg.Service.Auth.Password,
				Zone:     clusterCfg.Service.Zone,
				Type:     auth.ClientCredential,
			}
			dbnodeCredentials = append(dbnodeCredentials, nodeCredential)
		}

		etcdClusters := clusterCfg.Service.ETCDClusters
		for _, etcdCluster := range etcdClusters {
			if ok := etcdCluster.Auth.Enabled; ok {
				if err := etcdCluster.Auth.Validate(); err != nil {
					return err
				}
				etcdCredential := auth.OutboundCredentials{
					Username: etcdCluster.Auth.UserName,
					Password: etcdCluster.Auth.Password,
					Zone:     etcdCluster.Zone,
					Type:     auth.EtcdCredential,
				}
				etcdCredentails = append(etcdCredentails, etcdCredential)
			}
		}
	}
	auth.PopulateOutbound(dbnodeCredentials, etcdCredentails)
	return nil
}
