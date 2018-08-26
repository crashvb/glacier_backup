package com.vkleban.glacier_backup.config;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.vkleban.glacier_backup.InitException;

public class Config {
	
	@Expose(serialize = false , deserialize = false)
	private static Config config= null;
	
	@Expose(serialize = false , deserialize = false)
	private static Gson gson;
	
	// Root upload/download directory
	public String root_dir;
	public String region;
	public String vault;
	public String access_key;
	public String secret_key;
	public String sns_topic_arn;
	public long   polling_milliseconds;
	public String retrieval_tier;
    public String log_name;
	public String log_level;
	public int    log_files;
	public int    log_size;
	
	private Config(Path configPath) {}
	
	private static void enforceCoverage(String pathToObject, Object obj) throws InitException {
		for (Field field: obj.getClass().getDeclaredFields()) {
			try {
				// Skip all static and non-public fields
				if (field.getModifiers() != 1);
				// Error out for null field
				else if (field.get(obj) == null)
					throw new InitException("Missing config entry: \"" + pathToObject + field.getName() + "\"");
				// If it's list, process its elements
				else if (field.getType().equals(List.class)) {
					int index= 0;
					for (Object child : (List<?>) field.get(obj) ) {
						enforceCoverage(field.getName() + "[" + index + "]" + ".", child);
						index++;
					}
				} else {
					enforceCoverage(field.getName() + ".", field.get(obj));
				}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				throw new InitException("Error during configuration verification", e);
			}
		}
	}
	
	public static void init(Path configPath) throws InitException {
		try {
			gson= new GsonBuilder().setPrettyPrinting().create();
			byte[] configFile = Files.readAllBytes(configPath);
			config= gson.fromJson(new String(configFile, StandardCharsets.UTF_8), Config.class);
			enforceCoverage("", config);
		} catch (InitException e) {
			throw e;
		} catch (Exception e) {
			throw new InitException("Failed reading configuration file \"" + configPath + "\"", e);
		}
	}
	
	public static Config get() {
		if (config == null)
			throw new RuntimeException("Forgot to initialize properties. Please fix the code");
		return config;
	}
	
	
}
