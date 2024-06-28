package com.flipkart.fdp.superbi.cosmos.exception;

/**
 * Created by rajesh.kannan on 06/05/15.
 */
public class HttpException extends RuntimeException{

	private int statusCode;
	public static final int SC_INTERNAL_SERVER_ERROR_IDENTIFIED = 504;
	public static final int SC_FIELD_NOT_FOUND = 452;
	public static final int SC_META_DETAILS_EXPIRED = 453;
	public static final int SC_SESSION_EXPIRED = 440;
	public static final int SC_RESOURCE_UNAVAILABLE = 455;
	
	public HttpException(int statusCode) {
		super();
		this.statusCode = statusCode;
	}
	public HttpException(int statusCode, String message) {
		super(message);
		this.statusCode = statusCode;
	}
	public int getStatusCode() {
		return statusCode;
	}
}
