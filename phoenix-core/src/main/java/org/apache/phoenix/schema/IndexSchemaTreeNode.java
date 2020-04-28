package org.apache.phoenix.schema;

import java.sql.SQLException;

public class IndexSchemaTreeNode extends SchemaTreeNode {
    IndexSchemaTreeNode(PTable table, SchemaExtractionTool tool) {
        super(table, tool);
    }

    @Override
    void visit() throws SQLException {
        this.ddl = tool.extractCreateIndexDDL(table);
    }
}
