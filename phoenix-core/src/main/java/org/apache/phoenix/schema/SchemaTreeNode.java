package org.apache.phoenix.schema;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;

abstract class SchemaTreeNode {
    protected SchemaExtractionTool tool;
    protected PTable table;
    protected String ddl;
    protected ArrayList<SchemaTreeNode> children;

    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String DDL = "ddl";
    private static final String CHILDREN = "children";

    SchemaTreeNode(PTable table, SchemaExtractionTool tool) {
        this.tool = tool;
        this.table = table;
        this.children = new ArrayList<>();
    }

    abstract void visit() throws Exception;

    JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        json.put(NAME, table.getName().getString());
        json.put(TYPE, table.getType().toString());
        json.put(DDL, ddl);
        JSONArray jsonChildren = new JSONArray();
        for (SchemaTreeNode node : children) {
            jsonChildren.put(node.toJSON());
        }
        json.put(CHILDREN, jsonChildren);
        return json;
    }
}
