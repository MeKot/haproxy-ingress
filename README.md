# HAProxy Ingress controller

[Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) controller
implementation for [HAProxy](http://www.haproxy.org/) loadbalancer extended with a custom scaling and [brownout](https://dl.acm.org/doi/10.1145/2568225.2568227) controller.

The original [README](https://github.com/MeKot/haproxy-ingress/blob/feature/brownout/README_OLD.md) for the HAProxy-Ingress Controller

# What this is 

This is a modified version of the HAProxy-Ingress controller that implements the brownout paradigm. It 
extends the paradigm further by combining it with a replication controller. 

## This is a prototype that was developed for [QUDOS2020](http://2020.qudos-workshop.org/call_for_papers/)
