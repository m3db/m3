package namespaces

const (
	defaultPath = "/api/v1/namespace"
	debugQS     = "debug=true"
)

type NamespaceArgs struct {
	showAll *bool
	delete  *string
}

//func xCommand(flags *NamespaceArgs, endpoint string, log *zap.SugaredLogger) {
//	log.Debugf("NamespaceArgs:%+v:\n", flags)
//	url := fmt.Sprintf("%s%s?%s", endpoint, defaultPath, debugQS)
//	if *flags.showAll {
//		http.DoGet(url, log, http.DoDump)
//	} else if len(*flags.delete) > 0 {
//		url = fmt.Sprintf("%s%s/%s", endpoint, "/api/v1/services/m3db/namespace", *flags.delete)
//		http.DoDelete(url, log, http.DoDump)
//	} else {
//		http.DoGet(url, log, showNames)
//	}
//	return
//}
