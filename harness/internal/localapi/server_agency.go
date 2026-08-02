package localapi

import (
	"net/http"

	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func registerAgencyRoutes(mux *http.ServeMux, service Service) (bool, error) {
	agencyService, ok := service.(node.AgencyService)
	if !ok {
		return false, nil
	}
	agencyServer, err := NewAgencyServer(agencyService)
	if err != nil {
		return false, err
	}
	for _, route := range agencyRoutes() {
		mux.Handle(route, agencyServer)
	}
	return true, nil
}

func controlHandler(mux http.Handler, agencyAvailable bool) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !IsControlRoute(request.URL.Path) {
			writeErrorStatus(writer, http.StatusNotFound,
				NewAPIError(CodeInvalidArgument, "local control route does not exist"))
			return
		}
		if IsAgencyRoute(request.URL.Path) && !agencyAvailable {
			writeErrorStatus(writer, http.StatusNotFound,
				NewAPIError(CodeInvalidArgument, "local agency capability is unavailable"))
			return
		}
		mux.ServeHTTP(writer, request)
	})
}
