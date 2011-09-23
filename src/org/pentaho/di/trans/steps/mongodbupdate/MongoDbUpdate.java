 /* Copyright (c) 2007 Pentaho Corporation.  All rights reserved. 
 * This software was developed by Pentaho Corporation and is provided under the terms 
 * of the GNU Lesser General Public License, Version 2.1. You may not use 
 * this file except in compliance with the license. If you need a copy of the license, 
 * please go to http://www.gnu.org/licenses/lgpl-2.1.txt. The Original Code is Pentaho 
 * Data Integration.  The Initial Developer is Pentaho Corporation.
 *
 * Software distributed under the GNU Lesser Public License is distributed on an "AS IS" 
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or  implied. Please refer to 
 * the license for the specific language governing your rights and limitations.*/

/**
 * Reads a micro-cube type of data-file from disk.
 * It's just a binary (compressed) representation of a buch of rows.
 * 
 * @author Matt
 * @since 8-apr-2003
 */


package org.pentaho.di.trans.steps.mongodbupdate;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.mongodb.*;
import com.mongodb.util.*;
import org.bson.*;

public class MongoDbUpdate extends BaseStep implements StepInterface
{
	private static Class<?> PKG = MongoDbUpdateMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private MongoDbUpdateMeta meta;
	private MongoDbUpdateData data;
	
	public MongoDbUpdate(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	{
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	{
		Object[] row = getRow();

		if (row==null)
		{
			setOutputDone();
			return false;
		}
		if (first)
		{
			first=false;
			data.fieldNumber = meta.getFieldNames().length;
			data.fieldIndexes = new int[data.fieldNumber];			
			for (int i=0;i<data.fieldNumber;i++)
			{
				data.fieldIndexes[i] = getInputRowMeta().indexOfValue(meta.getFieldNames()[i]);
				if (data.fieldIndexes[i]<0)
				{
					logError(BaseMessages.getString(PKG, "MongoDbUpdate.Log.CanNotFindField",meta.getFieldNames()[i]));
					throw new KettleException(BaseMessages.getString(PKG, "MongoDbUpdate.Log.CanNotFindField",meta.getFieldNames()[i]));
				}
			}
		}
		String firstPrefix;
		String secondPrefix;
		boolean noContainer = false;
		if (meta.getContainerObjectName()==null) meta.setContinerObjectName("");
		if (meta.getContainerObjectName().length()==0)
		{
			firstPrefix = "";
			secondPrefix = "";
			meta.setArrayFlag(false);
			noContainer=true;
		} else 
		{
			firstPrefix = meta.getContainerObjectName()+".";
			secondPrefix = meta.getContainerObjectName()+(meta.getArrayFlag()?".$.":".");
		}
		DBObject globalQueryObj = new BasicDBObject();
		globalQueryObj.putAll(data.primaryQuery);
		DBObject queryObj = new BasicDBObject();
		DBObject updateInnerObj = new BasicDBObject();
		DBObject rowImageObj = new BasicDBObject();
		for (int i=0;i<data.fieldNumber;i++)
		{
			String fieldvalue=getInputRowMeta().getString(row,data.fieldIndexes[i]);
			if (meta.getKeyFieldFlags()[i])
			{
				queryObj.put(firstPrefix+meta.getJSONNames()[i],fieldvalue);
			} else 
			{
				updateInnerObj.put(secondPrefix+meta.getJSONNames()[i],fieldvalue);
			}			
			rowImageObj.put(meta.getJSONNames()[i],fieldvalue);
		}
		globalQueryObj.putAll(queryObj);
		boolean insert=false;
		if (meta.getInsertIfNotPresentFlag() && (data.collection.count(globalQueryObj) == 0) )
		{
			insert = true;
		}
		if (insert)
		{
			if (noContainer)
			{
				data.collection.insert(rowImageObj);
				if (getLogLevel().isDebug())
				{
					logDebug(BaseMessages.getString(PKG,"MongoDbUpdate.Log.DocumentInsert",JSON.serialize(rowImageObj)));
				}
			} else if (meta.getArrayFlag())
			{
				DBObject updateObj = new BasicDBObject();
				updateObj.put("$push",new BasicDBObject(meta.getContainerObjectName(),rowImageObj));
				data.collection.updateMulti(data.primaryQuery,updateObj);
				if (getLogLevel().isDebug())
				{
					logDebug(BaseMessages.getString(PKG,"MongoDbUpdate.Log.DocumentPushed",JSON.serialize(data.primaryQuery),JSON.serialize(updateObj)));
				}
			} else 
			{				
				DBObject updateObj = new BasicDBObject();
				updateObj.put("$set",new BasicDBObject(meta.getContainerObjectName(),rowImageObj));
				data.collection.updateMulti(data.primaryQuery,updateObj);
				if (getLogLevel().isDebug())
				{
					logDebug(BaseMessages.getString(PKG,"MongoDbUpdate.Log.InsertIntoDocument"),JSON.serialize(data.primaryQuery),JSON.serialize(updateObj));
				}
			}
			
		} else 
		{
			DBObject updateObj = new BasicDBObject();
			updateObj.put("$set",updateInnerObj);
			data.collection.updateMulti(globalQueryObj,updateObj);
			if (getLogLevel().isDebug())
			{
				logDebug(BaseMessages.getString(PKG,"MongoDbUpdate.Log.DocumentUpdated",JSON.serialize(globalQueryObj),JSON.serialize(updateObj)));
			}
		}
		putRow(getInputRowMeta(),row);
		return true;
	}

	public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface)
	{
		if (super.init(stepMetaInterface, stepDataInterface)) 
		{
			meta = (MongoDbUpdateMeta) stepMetaInterface;
			data = (MongoDbUpdateData) stepDataInterface;

			String hostname = environmentSubstitute(meta.getHostname());
			int port = Const.toInt(environmentSubstitute(meta.getPort()), 27017);
			String db = environmentSubstitute(meta.getDbName());
			String collection = environmentSubstitute(meta.getCollection());
			try 
			{
				data.primaryQuery = (DBObject)(JSON.parse(environmentSubstitute(meta.getPrimaryQuery())));
			} catch (Exception e)
			{
				logError(e.getMessage());
				data.primaryQuery = (DBObject)(JSON.parse("{}"));
			}
			try 
			{

				data.mongo = new Mongo(hostname, port);
				data.db = data.mongo.getDB(db);
		
				String realUser = environmentSubstitute(meta.getAuthenticationUser());
				String realPass = Encr.decryptPasswordOptionallyEncrypted(environmentSubstitute(meta.getAuthenticationPassword()));

				if (!Const.isEmpty(realUser) || !Const.isEmpty(realPass)) 
				{
					if (!data.db.authenticate(realUser, realPass.toCharArray())) 
					{
						throw new KettleException(BaseMessages.getString(PKG, "MongoDbUpdate.ErrorAuthenticating.Exception"));
					}
				}
				data.collection = data.db.getCollection(collection);
		
				return true;
			} catch (Exception e) 
			{
				logError(BaseMessages.getString(PKG, "MongoDbUpdate.ErrorConnectingToMongoDb.Exception", hostname, ""+port, db, collection), e);
				return false;
			}
		} else {
			return false;
		}
	}
	
	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) 
	{
		if (data.mongo!=null) 
		{
			data.mongo.close();
		}	 
		super.dispose(smi, sdi);
	}
	
}
