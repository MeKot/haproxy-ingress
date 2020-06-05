/*
Copyright 2019 The HAProxy Ingress Controller Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package types

import (
	"time"
)

// Metrics ...
type Metrics interface {
	HAProxyShowInfoResponseTime(duration time.Duration)
	HAProxySetServerResponseTime(duration time.Duration)
	ControllerProcTime(task string, duration time.Duration)
	AddIdleFactor(idle int)
	IncUpdateNoop()
	IncUpdateDynamic()
	IncUpdateFull()
	UpdateSuccessful(success bool)
	SetCertExpireDate(domain, cn string, notAfter *time.Time)
	IncCertSigningMissing(domains string, success bool)
	IncCertSigningExpiring(domains string, success bool)
	IncCertSigningOutdated(domains string, success bool)

	// Brownout-specific metrics
	SetBrownOutFeatureStatus(backend string, feature string, currentValue float64)
	SetBackendResponseTime(backend string, duration time.Duration)
}
