package com.flipkart.fdp.superbi.dsl.evaluators;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

/**
 * Created by rajesh.kannan on 17/06/15.
 */
public class JSScriptEngineAccessor {

	private static GenericObjectPool<ScriptEngine> pool;
	private static int MAX_ENGINES = 10;
	public static void initScriptEngine() {
		initScriptEngine(MAX_ENGINES);
	}
	public static void initScriptEngine(int concurrency) {
		if(pool == null) {
			GenericObjectPoolConfig config = new GenericObjectPoolConfig();
			config.setMaxTotal(concurrency);
			config.setMaxIdle(concurrency);
			pool = new GenericObjectPool<ScriptEngine>(new ScriptEnginePoolFactory(), config, new AbandonedConfig());
		}
	}

	public static class ScriptEnginePoolFactory
			implements PooledObjectFactory<ScriptEngine> {

		@Override public PooledObject<ScriptEngine> makeObject() throws Exception {
			return new DefaultPooledObject(new ScriptEngineManager(null).getEngineByName("js"));
		}

		@Override public void destroyObject(PooledObject<ScriptEngine> pooledObject)
				throws Exception {

		}

		@Override public boolean validateObject(PooledObject<ScriptEngine> pooledObject) {
			return true;
		}

		@Override public void activateObject(PooledObject<ScriptEngine> pooledObject) throws Exception {

		}

		@Override public void passivateObject(PooledObject<ScriptEngine> pooledObject) throws Exception {

		}
	}

	public static ScriptEngine borrowObject() {
		try {
			return pool.borrowObject();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void returnObject(ScriptEngine engine) {
		pool.returnObject(engine);
	}

	public static void invalidateObject(ScriptEngine engine) {
		try {
			pool.invalidateObject(engine);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}