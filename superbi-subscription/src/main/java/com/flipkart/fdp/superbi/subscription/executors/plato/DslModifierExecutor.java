package com.flipkart.fdp.superbi.subscription.executors.plato;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.subscription.model.plato.Canvas;

import java.io.IOException;
import java.util.List;

public class DslModifierExecutor {
    private final List<DslModifier> modifiers;

    public DslModifierExecutor(List<DslModifier> modifiers) {
        this.modifiers = modifiers;
    }

    public String executeModifiers(JsonNode rootNode, ObjectMapper mapper, Canvas.Tab.Widget widget) throws IOException {
        for (DslModifier modifier : modifiers) {
            modifier.modify(rootNode, mapper, widget);
        }
        return mapper.writeValueAsString(rootNode);
    }
}
