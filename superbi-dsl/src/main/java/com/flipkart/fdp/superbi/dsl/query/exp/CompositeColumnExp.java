package com.flipkart.fdp.superbi.dsl.query.exp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;

/**
 * Created by rajesh.kannan on 23/03/15.
 * It represents arithmetic(BODMAS) expression of columns used in where clause
 * eg colA + colB < 100
 */
public class CompositeColumnExp extends ColumnExp {

	public static final String COL_START_FMT = "$[";
	public static final String COL_END_FMT = "]$";

	@Getter
	private final String expr;

	@JsonCreator
	public CompositeColumnExp(@JsonProperty("expr") String expr) {
		super(expr);
		this.expr = expr;
	}

	public String convertToProperExpression(Function<String,String> columnFetcher)
	{
		String pattern = "\\$\\[([^\\]]*)]\\$";
		String nativeExpr = expr;

		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(expr);
		while(m.find()) {
			System.out.println("Found value: " + m.group(0));
			String givenColumn = m.group(1);
			String nativeColumn = columnFetcher.apply(givenColumn);
			nativeExpr = nativeExpr.replace(COL_START_FMT+givenColumn+COL_END_FMT, nativeColumn);
		}
		return nativeExpr;
	}

	@Override public String evaluateAndGetColName(
			Map<String, String[]> paramValues) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof CompositeColumnExp)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		CompositeColumnExp that = (CompositeColumnExp) o;
		return Objects.equal(expr, that.expr);
	}
}
