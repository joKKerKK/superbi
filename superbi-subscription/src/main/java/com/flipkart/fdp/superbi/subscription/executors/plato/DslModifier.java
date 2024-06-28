package com.flipkart.fdp.superbi.subscription.executors.plato;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.subscription.model.plato.Canvas;

public interface DslModifier {
    void modify(JsonNode rootNode, ObjectMapper mapper, Canvas.Tab.Widget widget);
}
