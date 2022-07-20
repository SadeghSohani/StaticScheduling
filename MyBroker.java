package org.cloudbus.cloudsim.examples.StaticScheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.examples.StaticScheduling.WorkFlowParser.Node;


public class MyBroker extends DatacenterBroker {

	private int provisionInterval = 120;
	
	private int completedTaskCount = 0;

	private int vmId = 0;

	private int datacenterId = 2;

	private ArrayList<Integer> idleVmIdList;
	private Map<Integer, Vm> id2VmMap;

	private Map<Integer, Double> id2VmStartTime;
	private Map<Integer, Double> id2VmTermiateTime;

	private HashMap<String, WorkFlowParser.Node> nodes;
	private WorkFlowParser workflow;
	private int num_tasks;
	private int workflow_depth;
	private int numberOfVms;
	private int numberOfVmsCreated = 0;

	private ArrayList<Cloudlet> taskList;
	private ArrayList<String> runningTaskIds;

	private HashMap<Integer, String> cloudletId2nodeId;

	private int cloudletId = 0;

	public MyBroker(String name) throws Exception {
		super(name);
	}

	@Override
	public void startEntity() {
		workflow = new WorkFlowParser("SIPHT_500_1.xml");
		idleVmIdList = new ArrayList<>();
		id2VmMap = new HashMap<>();
		taskList = new ArrayList<>();
		nodes = new HashMap<String, WorkFlowParser.Node>();
		cloudletId2nodeId = new HashMap<Integer,String>();
		nodes = workflow.getNodes();
		num_tasks = workflow.getNodes().size();
		workflow_depth = workflow.getDepth();
		Log.printLine("number of tasks = "+num_tasks);
		Log.printLine("workflow depth = "+workflow_depth);
		numberOfVms = (int) Math.ceil((float) num_tasks / workflow_depth);
		Log.printLine("VMs count = " + numberOfVms);
		runningTaskIds = new ArrayList<>();
		id2VmStartTime = new HashMap<>();
		id2VmTermiateTime = new HashMap<>();
		

		listPrepareTasks();

		for (int i = 0; i < numberOfVms; i++) {
			int newVmId = vmId++;
			int mips = 10;
			long size = 10000; // image size (MB)
			int ram = 512; // vm memory (MB)
			long bw = 1000;
			int pesNumber = 1; // number of cpus
			String vmm = "Xen"; // VMM name
	
			// create VM
			Vm vm = new Vm(newVmId, getId(), mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
			id2VmMap.put(newVmId, vm);
	
			// send request to datacenter
			Log.printLine(CloudSim.clock() + ": VM # " + vm.getId() + " is requested from datacenter");
			sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
		}

		
		
	}

	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {

		// VM Creation answer
		case CloudSimTags.VM_CREATE_ACK:
			processVmCreate(ev);
			scheduleTasks();

			break;
		// VM Creation answer
		case CloudSimTags.VM_DESTROY_ACK:
			processVmTerminate(ev);
			break;
		// A finished cloudlet returned
		case CloudSimTags.CLOUDLET_RETURN:
			processCloudletReturn(ev);
			listPrepareTasks();
			scheduleTasks();

			break;
		// if the simulation finishes
		case CloudSimTags.END_OF_SIMULATION:
			shutdownEntity();
			break;
			// other unknown tags are processed by this method
		default:
			processOtherEvent(ev);
			break;
		}
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		idleVmIdList.add(cloudlet.getVmId());

		String nodeId = cloudletId2nodeId.get(cloudlet.getCloudletId());
		nodes.get(nodeId).setTaskDone(true);

		Log.printLine(CloudSim.clock()+ ": "+ getName()+ ": Cloudlet "+ cloudlet.getCloudletId()+ " received");
		try {
			cloudlet.setCloudletStatus(Cloudlet.SUCCESS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		completedTaskCount ++;
		//listPrepareTasks();
		
		//if all tasks are executed terminate VMs
		if(completedTaskCount==num_tasks) {
			for(int vmId : id2VmMap.keySet()) {
				sendNow(datacenterId, CloudSimTags.VM_DESTROY_ACK, id2VmMap.get(vmId));
			}
		}
	}

	protected void listPrepareTasks(){

		for (Node node: nodes.values()) {
			
			List<String> parents = node.getParents();
			if(parents == null) {
				addCloudLet(node);
			} else {
				boolean isParentsDone = true;
				for(String parentId : parents) {
					if(!nodes.get(parentId).isTaskDone()){
						isParentsDone = false;
					}
				}
				if(isParentsDone){
					addCloudLet(node);
				}
			}

			
		}
	}

	protected void addCloudLet(Node node) {
		String nodeId = node.getId();
		if(!runningTaskIds.contains(nodeId)) {

			//long length = (long) (node.getRuntime()/100);
			cloudletId = cloudletId + 1;
			long length = 5000;
			long fileSize = 300;
			long outputSize = 300;
			int pesNumber = 1;
			UtilizationModel utilizationModel = new UtilizationModelFull();
			Cloudlet cloudlet = new Cloudlet(cloudletId, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel);
			cloudlet.setUserId(getId());
			taskList.add(cloudlet);
			runningTaskIds.add(nodeId);
			cloudletId2nodeId.put(cloudletId, nodeId);

			
		}
		
	}

	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];

		if (result == CloudSimTags.TRUE) {
			getVmsCreatedList().add(id2VmMap.get(vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId + " has been created in Datacenter #" +
					datacenterId);
			idleVmIdList.add(vmId);
			id2VmStartTime.put(vmId, CloudSim.clock());
			numberOfVmsCreated = numberOfVmsCreated + 1;
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId +
					" failed in Datacenter #" + datacenterId);
		}
	}

	protected void processVmTerminate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];

		if (result == CloudSimTags.TRUE) {
			getVmsCreatedList().add(id2VmMap.get(vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId +
					" has been terminated in Datacenter #" + datacenterId);
			id2VmTermiateTime.put(vmId, CloudSim.clock());
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Termination of VM #" + vmId +
					" failed in Datacenter #" + datacenterId);
		}
	}


	private void scheduleTasks() {

		if (taskList.size() == 0) {			
			return;
		}

		while (idleVmIdList.size() != 0 && taskList.size() != 0) {
			Cloudlet cloudlet = taskList.remove(0);
			int vmId = idleVmIdList.remove(0);
			assignTaskOnVm(cloudlet, id2VmMap.get(vmId));
		}

	}

	private void assignTaskOnVm(Cloudlet cloudlet, Vm vm) {
		cloudlet.setVmId(vm.getId());
		cloudlet.setUserId(getId());
		try {
			cloudlet.setCloudletStatus(Cloudlet.INEXEC);
		} catch (Exception e) {
			e.printStackTrace();
		}
		Log.printLine(CloudSim.clock() + ": cloudlet #" + cloudlet.getCloudletId() + "is scheduled on VM #" +
				vm.getId());
		sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
	}

	@Override
	public void shutdownEntity() {
		super.shutdownEntity();
		printResult();
	}

	private void printResult() {
		double vmPrice = 0.001;
		int cost = 0;

		for (int vmId : id2VmMap.keySet()) {
			double startTime = id2VmStartTime.get(vmId);
			double terminateTime = CloudSim.clock();
			if (id2VmTermiateTime.containsKey(vmId)) {
				terminateTime = id2VmTermiateTime.get(vmId);
			}
			double val = (terminateTime - startTime);

			//Log.printLine("time of vm #" + vmId + " = "+terminateTime);

			if(val < 600) {
				val = 600;
			}

			cost += val;
		}
		cost *= vmPrice;

		System.out.println("time: " + CloudSim.clock());
		System.out.println("budget: " + cost);

	}

}
