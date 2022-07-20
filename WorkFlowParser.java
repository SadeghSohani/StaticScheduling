package org.cloudbus.cloudsim.examples.StaticScheduling;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.cloudbus.cloudsim.Log;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

public class WorkFlowParser {
    
    private HashMap<String, Node> nodes = new HashMap<String, Node>();
    private List<Integer> rootNodesHeight = new ArrayList<Integer>();
    
    public WorkFlowParser(String xmlFilePath) {
        parseXmlFile(xmlFilePath);

        //Depth of workflow
        for (Node node : nodes.values()) {
            if(node.isEndTask()){
                workFlowDepth(node,1);
            }
        }
        

    }

    private void parseXmlFile(String path) {

        try {

            SAXBuilder builder = new SAXBuilder();
            //parse using builder to get DOM representation of the XML file
            Document dom = builder.build(new File(path));
            Element root = dom.getRootElement();
            List<Element> list = root.getChildren();
            for (Element node : list) {
                switch (node.getName().toLowerCase()) {
                    case "job":
                        String nodeId = node.getAttributeValue("id");
                        StringBuilder myStr = new StringBuilder(node.getAttributeValue("id"));
                        //String nodeId = myStr.toString().replace("ID","");
                        String nodeName = node.getAttributeValue("name");
                        String nodeNameSpace = node.getAttributeValue("namespace");
                        String nodeVersion = node.getAttributeValue("version");
                        /**
                         * capture runtime. If not exist, by default the runtime
                         * is 0.1. Otherwise CloudSim would ignore this task.
                         * BUG/#11
                         */
                        double nodeRuntime = 0.0;                       
                        if (node.getAttributeValue("runtime") != null) {
                            String nodeTime = node.getAttributeValue("runtime");
                            nodeRuntime = 1000 * Double.parseDouble(nodeTime);
                            if (nodeRuntime < 100) {
                                nodeRuntime = 100;
                            }
                        } else {
                            Log.printLine("Cannot find runtime for " + nodeName + ",set it to be 0");
                        }   //multiple the scale, by default it is 1.0
                        //length *= Parameters.getRuntimeScale();
                        List<Element> fileList = node.getChildren();
                        List<Uses> usesList = new ArrayList<Uses>();
                        for (Element file : fileList) {
                            if (file.getName().toLowerCase().equals("uses")) {
                                String fileName = file.getAttributeValue("name");
                                if (fileName == null) {
                                    fileName = file.getAttributeValue("file");
                                }
                                if (fileName == null) {
                                    Log.print("Error in parsing xml");
                                }

                                String link = file.getAttributeValue("link");
                                double size = 0.0;

                                String fileSize = file.getAttributeValue("size");
                                if (fileSize != null) {
                                    size = Double.parseDouble(fileSize) /*/ 1024*/;
                                } else {
                                    Log.printLine("File Size not found for " + fileName);
                                }

                                /**
                                 * a bug of cloudsim, size 0 causes a problem. 1
                                 * is ok.
                                 */
                                if (size == 0) {
                                    size++;
                                }
                                /**
                                 * Sets the file type 1 is input 2 is output
                                 */
                                String type = "" ;
                                if(file.getAttributeValue("type") != null){
                                    type = file.getAttributeValue("type");
                                } else {
                                    Log.printLine("uses type is null.");
                                }

                                Boolean register = true;
                                if(file.getAttributeValue("register") != null){
                                    register = Boolean.parseBoolean(file.getAttributeValue("register"));
                                }

                                Boolean transfer = true;
                                if(file.getAttributeValue("transfer") != null){
                                    transfer = Boolean.parseBoolean(file.getAttributeValue("transfer"));
                                }

                                Boolean optional = true;
                                if(file.getAttributeValue("optional") != null){
                                    optional = Boolean.parseBoolean(file.getAttributeValue("optional"));
                                }
                        

                                Uses uses = new Uses(fileName,link ,register,transfer,optional ,type, size);
                                usesList.add(uses);

                            }
                        }
                        
                        nodes.put(nodeId, new Node(nodeId,nodeNameSpace,nodeName,nodeVersion,nodeRuntime,usesList));

                        break;
                    
                    case "child":
                        List<Element> parentsList = node.getChildren();
                        String childName = node.getAttributeValue("ref");
                        if (nodes.containsKey(childName)) {

                            List<String> parents = new ArrayList<String>();

                            for (Element parent : parentsList) {
                                String parentId = parent.getAttributeValue("ref");
                                if (nodes.containsKey(parentId)) {
                                    parents.add(parentId);
                                    nodes.get(parentId).setEndTask(false);
                                }
                            }

                            nodes.get(childName).setParents(parents);

                        }
                        break;
                    
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Parsing Exception");
        }
            
    }

    public void workFlowDepth(Node node, int height){
        if(node.getParents() == null){
            if(nodes.get(node.getId()).getHeight() < height){
                nodes.get(node.getId()).setHeight(height);
            } 
            rootNodesHeight.add(nodes.get(node.getId()).getHeight());
        } else {
            for(String parentId : node.getParents()) {
                if(nodes.get(parentId).getHeight() < height){
                    nodes.get(parentId).setHeight(height);
                    workFlowDepth(nodes.get(parentId), height++);
                }
            }
        }
    }

    public HashMap<String, Node> getNodes() {
        return nodes;
    }

    public int getDepth(){
        int max = 0;
        for(int heigth : rootNodesHeight){
            if(heigth > max){
                max = heigth;
            }
        }
        return max;
    }

    public class Node{

        private String id;
        private String namespace;
        private String name;
        private String version;
        private Double runtime;
        private List<Uses> uses;
        private List<String> parents;
        private boolean endTask = true;
        private boolean taskDone = false;
        private int height = 0;

        public Node(String id, String namespace, String name, String version, Double runtime, List<Uses> uses) {
            this.id = id;
            this.namespace = namespace;
            this.name = name;
            this.version = version;
            this.runtime = runtime;
            this.uses = uses;
        }

        public String getId() {
            return id;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getName() {
            return name;
        }

        public String getVersion() {
            return version;
        }

        public Double getRuntime() {
            return runtime;
        }

        public List<Uses> getUses(){
            return uses;
        }

        public List<String> getParents() {
            return parents;
        }

        public boolean isEndTask(){
            return endTask;
        }

        public boolean isTaskDone(){
            return taskDone;
        }

        public int getHeight(){
            return height;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setNamespace(String namespace){
            this.namespace = namespace;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public void setRuntime(Double runtime) {
            this.runtime = runtime;
        }

        public void setUses(List<Uses> uses) {
            this.uses = uses;
        }

        public void setEndTask(boolean endTask) {
            this.endTask = endTask;
        }

        public void setTaskDone(boolean taskDone) {
            this.taskDone = taskDone;
        }

        public void setParents(List<String> parents) {
            this.parents = parents;
        }

        public void setHeight(int height) {
            this.height = height;
        }

        

    }

    public class Uses{
        private String file;
        private String link;
        private boolean register;
        private boolean transfer;
        private boolean optional;
        private String type;
        private Double size;

        public Uses(String file, String link, boolean register, boolean transfer, boolean optional, String type, Double size){
            this.file = file;
            this.link = link;
            this.register = register;
            this.transfer = transfer;
            this.optional = optional;
            this.type = type;
            this.size = size;
        }

        public String getFile(){
            return this.file;
        }

        public String getLink(){
            return this.link;
        }

        public boolean getRegister(){
            return this.register;
        }

        public boolean getTransfer(){
            return this.transfer;
        }

        public boolean getOptional(){
            return this.optional;
        }

        public String getType(){
            return this.type;
        }

        public Double getSize() {
            return this.size;
        }

        public void setFile(String file){
            this.file = file;
        }

        public void setLink(String link){
            this.link = link;
        }

        public void setRegister(boolean register){
            this.register = register;
        }

        public void setTransfer(boolean transfer){
            this.transfer = transfer;
        }

        public void setOptional(boolean optional) {
            this.optional = optional;
        }

        public void setType(String type){
            this.type = type;
        }

        public void setSize(Double size){
            this.size = size;
        }

    }

}