package com.flipkart.fdp.superbi.dsl.query.exp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import lombok.Getter;

/**
 * User: aniruddha.gangopadhyay
 * Date: 19/03/14
 * Time: 3:25 PM
 */
public class SelectColumnExp extends Exp {
    @Getter
    public final SelectColumn selectColumn;

    @JsonCreator
    public SelectColumnExp(@JsonProperty("selectColumn") SelectColumn selectColumn) {
        this.selectColumn = selectColumn;
    }

    public SelectColumnExp as(String alias){
        this.selectColumn.alias = alias;
        return this;
    }

    @Override
    protected Set<Param> getParameters() {
        return ImmutableSet.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SelectColumnExp)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SelectColumnExp that = (SelectColumnExp) o;
        return Objects.equal(getSelectColumn(), that.getSelectColumn());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), getSelectColumn());
    }
}
