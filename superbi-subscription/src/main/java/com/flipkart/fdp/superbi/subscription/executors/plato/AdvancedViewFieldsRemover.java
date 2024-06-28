package com.flipkart.fdp.superbi.subscription.executors.plato;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.subscription.model.plato.Canvas;
import com.flipkart.fdp.superbi.subscription.util.Constants;

public class AdvancedViewFieldsRemover implements DslModifier {
    @Override
    public void modify(JsonNode rootNode, ObjectMapper mapper, Canvas.Tab.Widget widget) {
        ObjectNode rootObjectNode = (ObjectNode) rootNode;
        if (rootObjectNode.has(Constants.ADVANCED_VIEW_FIELDS)) {
            rootObjectNode.remove(Constants.ADVANCED_VIEW_FIELDS);
        }
    }
}
