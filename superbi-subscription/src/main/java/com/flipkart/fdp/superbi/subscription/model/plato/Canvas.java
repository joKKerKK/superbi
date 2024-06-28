package com.flipkart.fdp.superbi.subscription.model.plato;

import com.google.common.base.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Canvas {
    private Optional<Long> id;
    private String alias;
    private String owner;

    private Boolean enabled;

    private Optional<Long> created;
    private Optional<Long> updated;
    private Optional<List<Tab>> tabs;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Tab {
        private Optional<Long> id;
        private Integer index;
        private String modelId;

        private Boolean enabled;

        private Optional<Long> created;
        private Optional<Long> updated;

        private Optional<String> alias;
        private Optional<List<Widget>> widgets;

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        public static class Widget {
            private Optional<Long> id;
            private Boolean enabled;
            private String alias;
            private String modelId;
            private String model;
            private Optional<Long> created;
            private Optional<Long> updated;
        }
    }
}
