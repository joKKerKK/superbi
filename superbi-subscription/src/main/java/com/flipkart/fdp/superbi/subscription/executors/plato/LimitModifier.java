package com.flipkart.fdp.superbi.subscription.executors.plato;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.subscription.model.plato.Canvas;
import com.flipkart.fdp.superbi.subscription.util.Constants;

public class LimitModifier implements DslModifier {


    @Override
    public void modify(JsonNode rootNode, ObjectMapper mapper, Canvas.Tab.Widget widget) {
        ObjectNode modelNode = (ObjectNode) rootNode.path(Constants.MODEL);
        if (modelNode.has(Constants.LIMIT)) {
            modelNode.putNull(Constants.LIMIT);
        } else {
            modelNode.set(Constants.LIMIT, null);
        }
    }
}
