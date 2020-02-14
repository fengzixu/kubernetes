package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/controller/nodelifecycle"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/plugin/pkg/admission/defaulttolerationseconds"
	"k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction"
	pluginapi "k8s.io/kubernetes/plugin/pkg/admission/podtolerationrestriction/apis/podtolerationrestriction"
	"k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

// TestTaintBasedEvictions tests related cases for the TaintBasedEvictions feature
func TestTaintBasedEvictions(t *testing.T) {
	// we need at least 2 nodes to prevent lifecycle manager from entering "fully-disrupted" mode
	nodeCount := 3
	zero := int64(0)
	gracePeriod := int64(1)
	heartbeatInternal := time.Second * 2
	testPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "testpod1", DeletionGracePeriodSeconds: &zero},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: "container", Image: imageutils.GetPauseImageName()},
			},
			Tolerations: []v1.Toleration{
				{
					Key:      v1.TaintNodeNotReady,
					Operator: v1.TolerationOpExists,
					Effect:   v1.TaintEffectNoExecute,
				},
			},
			TerminationGracePeriodSeconds: &gracePeriod,
		},
	}
	tolerationSeconds := []int64{200, 300, 0}
	tests := []struct {
		name                string
		nodeTaints          []v1.Taint
		nodeConditions      []v1.NodeCondition
		pod                 *v1.Pod
		waitForPodCondition string
	}{
		{
			name:                "Taint based evictions for NodeNotReady and 200 tolerationseconds",
			nodeTaints:          []v1.Taint{{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoExecute}},
			nodeConditions:      []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionFalse}},
			pod:                 testPod,
			waitForPodCondition: "updated with tolerationSeconds of 200",
		},
		{
			name:           "Taint based evictions for NodeNotReady with no pod tolerations",
			nodeTaints:     []v1.Taint{{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoExecute}},
			nodeConditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionFalse}},
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "testpod1"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "container", Image: imageutils.GetPauseImageName()},
					},
				},
			},
			waitForPodCondition: "updated with tolerationSeconds=300",
		},
		{
			name:                "Taint based evictions for NodeNotReady and 0 tolerationseconds",
			nodeTaints:          []v1.Taint{{Key: v1.TaintNodeNotReady, Effect: v1.TaintEffectNoExecute}},
			nodeConditions:      []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionFalse}},
			pod:                 testPod,
			waitForPodCondition: "terminating",
		},
		{
			name:           "Taint based evictions for NodeUnreachable",
			nodeTaints:     []v1.Taint{{Key: v1.TaintNodeUnreachable, Effect: v1.TaintEffectNoExecute}},
			nodeConditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionUnknown}},
		},
	}

	// Enable TaintBasedEvictions
	defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.TaintBasedEvictions, true)()

	// Build admission chain handler.
	podTolerations := podtolerationrestriction.NewPodTolerationsPlugin(&pluginapi.Configuration{})
	admission := admission.NewChainHandler(
		podTolerations,
		defaulttolerationseconds.NewDefaultTolerationSeconds(),
	)
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testCtx := initTestMaster(t, "taint-based-evictions", admission)

			// Build clientset and informers for controllers.
			externalClientset := kubernetes.NewForConfigOrDie(&restclient.Config{
				QPS:           -1,
				Host:          testCtx.httpServer.URL,
				ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
			externalInformers := informers.NewSharedInformerFactory(externalClientset, time.Second)
			podTolerations.SetExternalKubeClientSet(externalClientset)
			podTolerations.SetExternalKubeInformerFactory(externalInformers)

			testCtx = initTestScheduler(t, testCtx, true, nil)
			defer cleanupTest(t, testCtx)
			cs := testCtx.clientSet
			informers := testCtx.informerFactory
			_, err := cs.CoreV1().Namespaces().Create(context.TODO(), testCtx.ns, metav1.CreateOptions{})
			if err != nil {
				t.Errorf("Failed to create namespace %+v", err)
			}

			// Start NodeLifecycleController for taint.
			nc, err := nodelifecycle.NewNodeLifecycleController(
				informers.Coordination().V1().Leases(),
				informers.Core().V1().Pods(),
				informers.Core().V1().Nodes(),
				informers.Apps().V1().DaemonSets(),
				cs,
				5*time.Second,    // Node monitor grace period
				time.Minute,      // Node startup grace period
				time.Millisecond, // Node monitor period
				time.Second,      // Pod eviction timeout
				100,              // Eviction limiter QPS
				100,              // Secondary eviction limiter QPS
				50,               // Large cluster threshold
				0.55,             // Unhealthy zone threshold
				true,             // Run taint manager
				true,             // Use taint based evictions
			)
			if err != nil {
				t.Errorf("Failed to create node controller: %v", err)
				return
			}

			go nc.Run(testCtx.ctx.Done())

			// Waiting for all controller sync.
			externalInformers.Start(testCtx.ctx.Done())
			externalInformers.WaitForCacheSync(testCtx.ctx.Done())
			informers.Start(testCtx.ctx.Done())
			informers.WaitForCacheSync(testCtx.ctx.Done())

			nodeRes := v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("4000m"),
				v1.ResourceMemory: resource.MustParse("16Gi"),
				v1.ResourcePods:   resource.MustParse("110"),
			}

			var nodes []*v1.Node
			for i := 0; i < nodeCount; i++ {
				nodes = append(nodes, &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name:   fmt.Sprintf("node-%d", i),
						Labels: map[string]string{v1.LabelZoneRegion: "region1", v1.LabelZoneFailureDomain: "zone1"},
					},
					Spec: v1.NodeSpec{},
					Status: v1.NodeStatus{
						Capacity:    nodeRes,
						Allocatable: nodeRes,
						Conditions: []v1.NodeCondition{
							{
								Type:              v1.NodeReady,
								Status:            v1.ConditionTrue,
								LastHeartbeatTime: metav1.Now(),
							},
						},
					},
				})
				if _, err := cs.CoreV1().Nodes().Create(context.TODO(), nodes[i], metav1.CreateOptions{}); err != nil {
					t.Errorf("Failed to create node, err: %v", err)
				}
			}

			neededNode := nodes[1]
			if test.pod != nil {
				test.pod.Name = fmt.Sprintf("testpod-%d", i)
				if len(test.pod.Spec.Tolerations) > 0 {
					test.pod.Spec.Tolerations[0].TolerationSeconds = &tolerationSeconds[i]
				}

				test.pod, err = cs.CoreV1().Pods(testCtx.ns.Name).Create(context.TODO(), test.pod, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Test Failed: error: %v, while creating pod", err)
				}

				if err := waitForPodToSchedule(cs, test.pod); err != nil {
					t.Errorf("Failed to schedule pod %s/%s on the node, err: %v",
						test.pod.Namespace, test.pod.Name, err)
				}
				test.pod, err = cs.CoreV1().Pods(testCtx.ns.Name).Get(context.TODO(), test.pod.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Test Failed: error: %v, while creating pod", err)
				}
				neededNode, err = cs.CoreV1().Nodes().Get(context.TODO(), test.pod.Spec.NodeName, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Error while getting node associated with pod %v with err %v", test.pod.Name, err)
				}
			}

			// Regularly send heartbeat event to APIServer so that the cluster doesn't enter fullyDisruption mode.
			// TODO(Huang-Wei): use "NodeDisruptionExclusion" feature to simply the below logic when it's beta.
			for i := 0; i < nodeCount; i++ {
				var conditions []v1.NodeCondition
				// If current node is not <neededNode>
				if neededNode.Name != nodes[i].Name {
					conditions = []v1.NodeCondition{
						{
							Type:   v1.NodeReady,
							Status: v1.ConditionTrue,
						},
					}
				} else {
					c, err := nodeReadyStatus(test.nodeConditions)
					if err != nil {
						t.Error(err)
					}
					// Need to distinguish NodeReady/False and NodeReady/Unknown.
					// If we try to update the node with condition NotReady/False, i.e. expect a NotReady:NoExecute taint
					// we need to keep sending the update event to keep it alive, rather than just sending once.
					if c == v1.ConditionFalse {
						conditions = test.nodeConditions
					} else if c == v1.ConditionUnknown {
						// If it's expected to update the node with condition NotReady/Unknown,
						// i.e. expect a Unreachable:NoExecute taint,
						// we need to only send the update event once to simulate the network unreachable scenario.
						nodeCopy := nodeCopyWithConditions(nodes[i], test.nodeConditions)
						if err := updateNodeStatus(cs, nodeCopy); err != nil && !apierrors.IsNotFound(err) {
							t.Errorf("Cannot update node: %v", err)
						}
						continue
					}
				}
				// Keeping sending NodeReady/True or NodeReady/False events.
				go func(i int) {
					for {
						select {
						case <-testCtx.ctx.Done():
							return
						case <-time.Tick(heartbeatInternal):
							nodeCopy := nodeCopyWithConditions(nodes[i], conditions)
							if err := updateNodeStatus(cs, nodeCopy); err != nil && !apierrors.IsNotFound(err) {
								t.Errorf("Cannot update node: %v", err)
							}
						}
					}
				}(i)
			}

			if err := waitForNodeTaints(cs, neededNode, test.nodeTaints); err != nil {
				t.Errorf("Failed to taint node in test %d <%s>, err: %v", i, neededNode.Name, err)
			}

			if test.pod != nil {
				err = pod.WaitForPodCondition(cs, testCtx.ns.Name, test.pod.Name, test.waitForPodCondition, time.Second*15, func(pod *v1.Pod) (bool, error) {
					// as node is unreachable, pod0 is expected to be in Terminating status
					// rather than getting deleted
					if tolerationSeconds[i] == 0 {
						return pod.DeletionTimestamp != nil, nil
					}
					if seconds, err := getTolerationSeconds(pod.Spec.Tolerations); err == nil {
						return seconds == tolerationSeconds[i], nil
					}
					return false, nil
				})
				if err != nil {
					pod, _ := cs.CoreV1().Pods(testCtx.ns.Name).Get(context.TODO(), test.pod.Name, metav1.GetOptions{})
					t.Fatalf("Error: %v, Expected test pod to be %s but it's %v", err, test.waitForPodCondition, pod)
				}
				cleanupPods(cs, t, []*v1.Pod{test.pod})
			}
			cleanupNodes(cs, t)
			waitForSchedulerCacheCleanup(testCtx.scheduler, t)
		})
	}
}
