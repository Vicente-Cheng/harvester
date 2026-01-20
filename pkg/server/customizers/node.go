package customizers

import (
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/schema"
	"github.com/sirupsen/logrus"
)

/*
	 wrangler summarizers generate a generating warning as follows and we need to hide it if needed
		{
		  "error": false,
		  "message": "VirtualMachine generation is 5, but latest observed generation is 4",
		  "name": "in-progress",
		  "transitioning": true
		}
*/

func showReadyForState(request *types.APIRequest, resource *types.RawResource) {
	data := resource.APIObject.Data()
	conds := data.Slice("status", "conditions")
	state := data.Map("metadata", "state")
	logrus.Infof("[VICENTE DBG]: found state %v\n", state)
	logrus.Infof("[VICENTE DBG]: found conditions %v\n", conds)
}

var NodeCustomizerTemplate = schema.Template{
	Group:     "",
	Kind:      "node",
	Formatter: showReadyForState,
}
