package org.cruxframework.crux.plugin.appengine.rest;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cruxframework.crux.core.server.rest.state.ResourceStateHandler;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

public class AppengineResourceStateHandler implements ResourceStateHandler 
{
	private static final Log logger = LogFactory.getLog(AppengineResourceStateHandler.class);
	private static final String CACHE_REST = "_CacheCruxRest_";

	public static class CacheEntry implements ResourceState, Serializable
	{
        private static final long serialVersionUID = -8898489492776595483L;
		private final long dateModifiedMilis;
		private final long expires;
		private final String etag;

		private CacheEntry(long dateModifiedMilis, long expires, String etag)
		{
			this.dateModifiedMilis = dateModifiedMilis;
			this.expires = expires;
			this.etag = etag;
		}

		@Override
		public long getDateModified()
		{
			return dateModifiedMilis;
		}

		@Override
		public String getEtag()
		{
			return etag;
		}

		@Override
		public boolean isExpired()
		{
			return System.currentTimeMillis() - dateModifiedMilis >= expires;
		}
	}
	
    @Override
    public ResourceState add(String uri, long dateModified, long expires, String etag)
    {
		CacheEntry entry = new CacheEntry(dateModified, expires, etag);
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		Key restConfigKey = checkRestConfigKey(datastore);
		datastore.put(toEntity(uri, entry, restConfigKey));
		getCache().put(uri, entry, Expiration.onDate(new Date(expires)));
	    return entry;
    }

	@Override
    public ResourceState get(String uri)
    {
		CacheEntry entry = (CacheEntry) getCache().get(uri);
	    if (entry == null)
	    {
			DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
			Key restConfigKey = checkRestConfigKey(datastore);
			Key key = KeyFactory.createKey(restConfigKey, "_CruxRestConfigEntry_", uri);
			try
			{
				Entity entity = datastore.get(key);
				if (entity != null)
				{
					entry = fromEntity(entity);
					if (entry.isExpired())
					{
						entry = null;
					}
					else
					{
						getCache().put(uri, entry, Expiration.onDate(new Date(entry.expires)));
					}
				}
			}
			catch (EntityNotFoundException e)
			{
				// entry is null;
			}
	    }
		
	    return entry;
    }

	@Override
    public void remove(String uri)
    {
		getCache().delete(uri);
	    
    }

	@Override
	public void removeSegments(String... baseURIs)
	{
	    // TODO Auto-generated method stub
	    
	}

	@Override
    public void clear()
    {
		getCache().clearAll();
    }

	private CacheEntry fromEntity(Entity entity)
	{
		String etag = (String) entity.getProperty("etag");
		Date expires = (Date) entity.getProperty("expires");
		Long dateModifiedMilis = (Long) entity.getProperty("dateModifiedMilis");
		CacheEntry entry = new CacheEntry(dateModifiedMilis, expires.getTime(), etag);
		return entry;
	}
	
	private Entity toEntity(String uri, CacheEntry entry, Key restConfigKey)
	{
		Entity entity = new Entity("_CruxRestConfigEntry_", uri, restConfigKey);
		entity.setUnindexedProperty("etag", entry.etag);
		entity.setUnindexedProperty("expires", new Date(entry.expires));
		entity.setUnindexedProperty("dateModifiedMilis", entry.dateModifiedMilis);
		
		return entity;
	}
	
	private Key checkRestConfigKey(DatastoreService datastore)
    {
	    Key configBoardKey = KeyFactory.createKey("_CruxRestConfig_", "appConfigurations");
		try
        {
	        datastore.get(configBoardKey);
        }
        catch (EntityNotFoundException e)
        {
    		Transaction txn = datastore.beginTransaction();
    		try
    		{
    			configBoardKey = datastore.put(new Entity("_CruxRestConfig_", "appConfigurations"));
    			txn.commit();
    		}
    		finally
    		{
    		    if (txn.isActive()) 
    		    {
    		        txn.rollback();
    		    }
    		}
        }
	    return configBoardKey;
    }
	
	/**
	 * 
	 * @return
	 */
	private MemcacheService getCache()
	{
		return MemcacheServiceFactory.getMemcacheService(CACHE_REST);	
	}		
}
