package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	schedulerapiv1 "k8s.io/kubernetes/pkg/scheduler/apis/config/v1"
	taintutils "k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/kubernetes/test/integration/framework"
)

type testContext struct {
	closeFn         framework.CloseFunc
	httpServer      *httptest.Server
	ns              *v1.Namespace
	clientSet       *clientset.Clientset
	informerFactory informers.SharedInformerFactory
	scheduler       *scheduler.Scheduler
	ctx             context.Context
	cancelFn        context.CancelFunc
}

func cleanupNodes(cs clientset.Interface, t *testing.T) {
	err := cs.CoreV1().Nodes().DeleteCollection(context.TODO(), metav1.NewDeleteOptions(0), metav1.ListOptions{})
	if err != nil {
		t.Errorf("error while deleting all nodes: %v", err)
	}
}

// podDeleted returns true if a pod is not found in the given namespace.
func podDeleted(c clientset.Interface, podNamespace, podName string) wait.ConditionFunc {
	return func() (bool, error) {
		pod, err := c.CoreV1().Pods(podNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if pod.DeletionTimestamp != nil {
			return true, nil
		}
		return false, nil
	}
}

func cleanupTest(t *testing.T, testCtx *testContext) {
	// Kill the scheduler.
	testCtx.cancelFn()
	// Cleanup nodes.
	testCtx.clientSet.CoreV1().Nodes().DeleteCollection(context.TODO(), nil, metav1.ListOptions{})
	framework.DeleteTestingNamespace(testCtx.ns, testCtx.httpServer, t)
	testCtx.closeFn()
}

// cleanupPods deletes the given pods and waits for them to be actually deleted.
func cleanupPods(cs clientset.Interface, t *testing.T, pods []*v1.Pod) {
	for _, p := range pods {
		err := cs.CoreV1().Pods(p.Namespace).Delete(context.TODO(), p.Name, metav1.NewDeleteOptions(0))
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("error while deleting pod %v/%v: %v", p.Namespace, p.Name, err)
		}
	}
	for _, p := range pods {
		if err := wait.Poll(time.Millisecond, wait.ForeverTestTimeout,
			podDeleted(cs, p.Namespace, p.Name)); err != nil {
			t.Errorf("error while waiting for pod  %v/%v to get deleted: %v", p.Namespace, p.Name, err)
		}
	}
}

func addTaintToNode(cs clientset.Interface, nodeName string, taint v1.Taint) error {
	node, err := cs.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	copy := node.DeepCopy()
	copy.Spec.Taints = append(copy.Spec.Taints, taint)
	_, err = cs.CoreV1().Nodes().Update(context.TODO(), copy, metav1.UpdateOptions{})
	return err
}

// waitForNodeTaints waits for a node to have the target taints and returns
// an error if it does not have taints within the given timeout.
func waitForNodeTaints(cs clientset.Interface, node *v1.Node, taints []v1.Taint) error {
	return wait.Poll(100*time.Millisecond, 30*time.Second, nodeTainted(cs, node.Name, taints))
}

// nodeTainted return a condition function that returns true if the given node contains
// the taints.
func nodeTainted(cs clientset.Interface, nodeName string, taints []v1.Taint) wait.ConditionFunc {
	return func() (bool, error) {
		node, err := cs.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		// node.Spec.Taints may have more taints
		if len(taints) > len(node.Spec.Taints) {
			return false, nil
		}

		for _, taint := range taints {
			if !taintutils.TaintExists(node.Spec.Taints, &taint) {
				return false, nil
			}
		}

		return true, nil
	}
}

// nodeReadyStatus returns the status of first condition with type NodeReady.
// If none of the condition is of type NodeReady, returns an error.
func nodeReadyStatus(conditions []v1.NodeCondition) (v1.ConditionStatus, error) {
	for _, c := range conditions {
		if c.Type != v1.NodeReady {
			continue
		}
		// Just return the first condition with type NodeReady
		return c.Status, nil
	}
	return v1.ConditionFalse, errors.New("None of the conditions is of type NodeReady")
}

func getTolerationSeconds(tolerations []v1.Toleration) (int64, error) {
	for _, t := range tolerations {
		if t.Key == v1.TaintNodeNotReady && t.Effect == v1.TaintEffectNoExecute && t.Operator == v1.TolerationOpExists {
			return *t.TolerationSeconds, nil
		}
	}
	return 0, fmt.Errorf("cannot find toleration")
}

func nodeCopyWithConditions(node *v1.Node, conditions []v1.NodeCondition) *v1.Node {
	copy := node.DeepCopy()
	copy.ResourceVersion = "0"
	copy.Status.Conditions = conditions
	for i := range copy.Status.Conditions {
		copy.Status.Conditions[i].LastHeartbeatTime = metav1.Now()
	}
	return copy
}

// updateNodeStatus updates the status of node.
func updateNodeStatus(cs clientset.Interface, node *v1.Node) error {
	_, err := cs.CoreV1().Nodes().UpdateStatus(context.TODO(), node, metav1.UpdateOptions{})
	return err
}

// initTestMasterAndScheduler initializes a test environment and creates a master with default
// configuration.
func initTestMaster(t *testing.T, nsPrefix string, admission admission.Interface) *testContext {
	ctx, cancelFunc := context.WithCancel(context.Background())
	testCtx := testContext{
		ctx:      ctx,
		cancelFn: cancelFunc,
	}

	// 1. Create master
	h := &framework.MasterHolder{Initialized: make(chan struct{})}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		<-h.Initialized
		h.M.GenericAPIServer.Handler.ServeHTTP(w, req)
	}))

	masterConfig := framework.NewIntegrationTestMasterConfig()

	if admission != nil {
		masterConfig.GenericConfig.AdmissionControl = admission
	}

	_, testCtx.httpServer, testCtx.closeFn = framework.RunAMasterUsingServer(masterConfig, s, h)

	if nsPrefix != "default" {
		testCtx.ns = framework.CreateTestingNamespace(nsPrefix+string(uuid.NewUUID()), s, t)
	} else {
		testCtx.ns = framework.CreateTestingNamespace("default", s, t)
	}

	// 2. Create kubeclient
	testCtx.clientSet = clientset.NewForConfigOrDie(
		&restclient.Config{
			QPS: -1, Host: s.URL,
			ContentConfig: restclient.ContentConfig{
				GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"},
			},
		},
	)
	return &testCtx
}

func waitForSchedulerCacheCleanup(sched *scheduler.Scheduler, t *testing.T) {
	schedulerCacheIsEmpty := func() (bool, error) {
		dump := sched.Cache().Dump()

		return len(dump.Nodes) == 0 && len(dump.AssumedPods) == 0, nil
	}

	if err := wait.Poll(time.Second, wait.ForeverTestTimeout, schedulerCacheIsEmpty); err != nil {
		t.Errorf("Failed to wait for scheduler cache cleanup: %v", err)
	}
}

// initTestScheduler initializes a test environment and creates a scheduler with default
// configuration.
func initTestScheduler(
	t *testing.T,
	testCtx *testContext,
	setPodInformer bool,
	policy *schedulerapi.Policy,
) *testContext {
	// Pod preemption is enabled by default scheduler configuration.
	return initTestSchedulerWithOptions(t, testCtx, setPodInformer, policy, time.Second)
}

// initTestSchedulerWithOptions initializes a test environment and creates a scheduler with default
// configuration and other options.
func initTestSchedulerWithOptions(
	t *testing.T,
	testCtx *testContext,
	setPodInformer bool,
	policy *schedulerapi.Policy,
	resyncPeriod time.Duration,
	opts ...scheduler.Option,
) *testContext {
	// 1. Create scheduler
	testCtx.informerFactory = informers.NewSharedInformerFactory(testCtx.clientSet, resyncPeriod)

	var podInformer coreinformers.PodInformer

	// create independent pod informer if required
	if setPodInformer {
		podInformer = scheduler.NewPodInformer(testCtx.clientSet, 12*time.Hour)
	} else {
		podInformer = testCtx.informerFactory.Core().V1().Pods()
	}
	var err error
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{
		Interface: testCtx.clientSet.EventsV1beta1().Events(""),
	})
	recorder := eventBroadcaster.NewRecorder(
		legacyscheme.Scheme,
		v1.DefaultSchedulerName,
	)
	if policy != nil {
		opts = append(opts, scheduler.WithAlgorithmSource(createAlgorithmSourceFromPolicy(policy, testCtx.clientSet)))
	}
	opts = append([]scheduler.Option{scheduler.WithBindTimeoutSeconds(600)}, opts...)
	testCtx.scheduler, err = scheduler.New(
		testCtx.clientSet,
		testCtx.informerFactory,
		podInformer,
		recorder,
		testCtx.ctx.Done(),
		opts...,
	)

	if err != nil {
		t.Fatalf("Couldn't create scheduler: %v", err)
	}

	// set setPodInformer if provided.
	if setPodInformer {
		go podInformer.Informer().Run(testCtx.scheduler.StopEverything)
		cache.WaitForNamedCacheSync("scheduler", testCtx.scheduler.StopEverything, podInformer.Informer().HasSynced)
	}

	stopCh := make(chan struct{})
	eventBroadcaster.StartRecordingToSink(stopCh)

	testCtx.informerFactory.Start(testCtx.scheduler.StopEverything)
	testCtx.informerFactory.WaitForCacheSync(testCtx.scheduler.StopEverything)

	go testCtx.scheduler.Run(testCtx.ctx)

	return testCtx
}

func createAlgorithmSourceFromPolicy(policy *schedulerapi.Policy, clientSet clientset.Interface) schedulerapi.SchedulerAlgorithmSource {
	// Serialize the Policy object into a ConfigMap later.
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), runtime.ContentTypeJSON)
	if !ok {
		panic("could not find json serializer")
	}
	encoder := scheme.Codecs.EncoderForVersion(info.Serializer, schedulerapiv1.SchemeGroupVersion)
	policyString := runtime.EncodeOrDie(encoder, policy)
	configPolicyName := "scheduler-custom-policy-config"
	policyConfigMap := v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: configPolicyName},
		Data:       map[string]string{schedulerapi.SchedulerPolicyConfigMapKey: policyString},
	}
	policyConfigMap.APIVersion = "v1"
	clientSet.CoreV1().ConfigMaps(metav1.NamespaceSystem).Create(context.TODO(), &policyConfigMap, metav1.CreateOptions{})

	return schedulerapi.SchedulerAlgorithmSource{
		Policy: &schedulerapi.SchedulerPolicySource{
			ConfigMap: &schedulerapi.SchedulerPolicyConfigMapSource{
				Namespace: policyConfigMap.Namespace,
				Name:      policyConfigMap.Name,
			},
		},
	}
}

// waitForPodToScheduleWithTimeout waits for a pod to get scheduled and returns
// an error if it does not scheduled within the given timeout.
func waitForPodToScheduleWithTimeout(cs clientset.Interface, pod *v1.Pod, timeout time.Duration) error {
	return wait.Poll(100*time.Millisecond, timeout, podScheduled(cs, pod.Namespace, pod.Name))
}

// waitForPodToSchedule waits for a pod to get scheduled and returns an error if
// it does not get scheduled within the timeout duration (30 seconds).
func waitForPodToSchedule(cs clientset.Interface, pod *v1.Pod) error {
	return waitForPodToScheduleWithTimeout(cs, pod, 30*time.Second)
}

func podScheduled(c clientset.Interface, podNamespace, podName string) wait.ConditionFunc {
	return func() (bool, error) {
		pod, err := c.CoreV1().Pods(podNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			// This could be a connection error so we want to retry.
			return false, nil
		}
		if pod.Spec.NodeName == "" {
			return false, nil
		}
		return true, nil
	}
}
