package com.flipkart.fdp.superbi.subscription.executors.plato;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.subscription.model.plato.Canvas;
import com.flipkart.fdp.superbi.subscription.util.Constants;

import java.util.HashMap;
import java.util.Map;

public class MetadataModifier implements DslModifier {
    @Override
    public void modify(JsonNode rootNode, ObjectMapper mapper, Canvas.Tab.Widget widget) {
        ObjectNode rootObjectNode = (ObjectNode) rootNode;

        ObjectNode metadataNode;
        if (rootObjectNode.has(Constants.METADATA)) {
            metadataNode = (ObjectNode) rootObjectNode.path(Constants.METADATA);
            metadataNode.removeAll();
        } else {
            metadataNode = mapper.createObjectNode();
        }

        ObjectNode resourceInfoNode = mapper.createObjectNode();
        Map<String, String> resourceInfo = new HashMap<>();
        if (widget.getId().isPresent()) {
            resourceInfo.put(Constants.WIDGET_ID, widget.getId().get().toString());
        }

        resourceInfo.forEach(resourceInfoNode::put);
        metadataNode.set(Constants.RESOURCE_INFO, resourceInfoNode);

        rootObjectNode.set(Constants.METADATA, metadataNode);
    }
}
