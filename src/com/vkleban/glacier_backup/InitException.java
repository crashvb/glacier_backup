package com.vkleban.glacier_backup;

public class InitException extends Exception {
	
	public InitException(String string) {
		super(string);
	}

	public InitException(String string, Exception e) {
		super(string, e);
	}

	/**
	 * Java made me do it. This is random number for serialization
	 */
	private static final long serialVersionUID = 2280971623134701063L;

}
