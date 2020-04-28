package org.apache.phoenix.schema;

public class TableSchemaTreeNode extends SchemaTreeNode {

    TableSchemaTreeNode(PTable table, SchemaExtractionTool tool) {
        super(table, tool);
    }

    @Override
    void visit() throws Exception {

        this.ddl = tool.extractCreateTableDDL(table);

        // get all indices
        for (PTable index : table.getIndexes()) {
            children.add(new IndexSchemaTreeNode(index, tool));
        }

        // TODO: get all views for this table and add to children

        for (SchemaTreeNode node : children) {
            node.visit();
        }
    }
}
