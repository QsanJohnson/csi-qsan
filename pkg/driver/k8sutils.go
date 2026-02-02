package driver

import (
	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

func getNodeIds() []string {
	var nodeIds []string

	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Errorf("[getNodeIds] InClusterConfig failed, err: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Errorf("[getNodeIds] NewForConfig failed, err: %v", err)
	}
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("[getNodeIds] List nodes failed, err: %v", err)
	}

	for _, node := range nodes.Items {
		nodeIds = append(nodeIds, node.Name)
	}

	return nodeIds
}

func getNodeLabels() map[string]map[string]string {
	nodeLabels := make(map[string]map[string]string)

	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Errorf("[getNodeLabels] InClusterConfig failed, err: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Errorf("[getNodeLabels] NewForConfig failed, err: %v", err)
	}
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("[getNodeLabels] List nodes failed, err: %v", err)
	}

	for _, node := range nodes.Items {
		nodeLabels[node.Name] = node.Labels
	}

	return nodeLabels
}
