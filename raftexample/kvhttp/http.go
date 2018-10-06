package kvhttp

import (
	"github.com/emicklei/go-restful"

	"net/http"
)




func (k *KvService) GetKV(request *restful.Request, response *restful.Response) {
}

func (k *KvService) PutKV(request *restful.Request, response *restful.Response) {
}

func (k *KvService) DeleteKV(request *restful.Request, response *restful.Response) {
}

func (k *KvService) ConfChange(request *restful.Request, response *restful.Response) {
}

func NewNode(webaddr string) {

	kv := newKvService()

	wsContainer := restful.NewContainer()
	ws := new(restful.WebService)
	wsContainer.Add(ws)
	ws.Route(ws.GET("/").To(kv.GetKV))
	ws.Route(ws.PUT("/").To(kv.PutKV))
	ws.Route(ws.DELETE("/").To(kv.DeleteKV))
	ws.Route(ws.POST("/").To(kv.ConfChange))
	server := &http.Server{Addr: webaddr, Handler: wsContainer}
	server.ListenAndServe()
}
