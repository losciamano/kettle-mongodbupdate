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


package org.pentaho.di.trans.steps.mongodbupdate;

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * Created on 8-apr-2011
 * @author matt
 * @since 4.2.0-M1
 */
public class MongoDbUpdateMeta extends BaseStepMeta implements StepMetaInterface
{
	private static Class<?> PKG = MongoDbUpdateMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	public static final String[] TrimTypes= {
		BaseMessages.getString(PKG,"MongoDbUpdateMeta.Trim.None"),
		BaseMessages.getString(PKG,"MongoDbUpdateMeta.Trim.Left"),
		BaseMessages.getString(PKG,"MongoDbUpdateMeta.Trim.Right"),
		BaseMessages.getString(PKG,"MongoDbUpdateMeta.Trim.Full"),
	};

	private String hostname;
	private String port;
	private String dbName;
	private String collection;
	private String primaryQuery;
	private String fieldNames[];
	private boolean keyFlags[];
	private String jsonNames[];
	private String containerObject;
	private boolean arrayFlag;	  
	private String authenticationUser;
	private String authenticationPassword;
	private boolean insertIfNotPresent;
	private int trimType;


	public MongoDbUpdateMeta()
	{
		super(); // allocate BaseStepMeta
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String,Counter> counters)
		throws KettleXMLException
	{
		readData(stepnode);
	}

	public Object clone()
	{
		MongoDbUpdateMeta retval = (MongoDbUpdateMeta)super.clone();
		return retval;
	}
	
	private void readData(Node stepnode)
		throws KettleXMLException
	{
		try
		{
			hostname  = XMLHandler.getTagValue(stepnode, "hostname"); //$NON-NLS-1$ //$NON-NLS-2$
			port = XMLHandler.getTagValue(stepnode, "port"); //$NON-NLS-1$ //$NON-NLS-2$
			dbName = XMLHandler.getTagValue(stepnode, "db_name"); //$NON-NLS-1$
			collection  = XMLHandler.getTagValue(stepnode, "collection"); //$NON-NLS-1$
			arrayFlag = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "array_flag"));//$NON-NLS-1$
			insertIfNotPresent = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "insert_flag"));//$NON-NLS-1$
			primaryQuery = XMLHandler.getTagValue(stepnode,"primary_query"); //$NON-NLS-1$
			containerObject = XMLHandler.getTagValue(stepnode,"container_object");//$NON-NLS-1$
			authenticationUser = XMLHandler.getTagValue(stepnode,"auth_user"); //$NON-NLS-1$
			authenticationPassword = Encr.decryptPasswordOptionallyEncrypted(XMLHandler.getTagValue(stepnode,"auth_password"));//$NON-NLS-1$
			trimType = Const.toInt(XMLHandler.getTagValue(stepnode,"trim_type"),0);//$NON-NLS-1$
			Node fields = XMLHandler.getSubNode(stepnode,"fields");
			int numFields = XMLHandler.countNodes(fields,"field");
			allocate(numFields);
			for (int k=0;k<numFields;k++)
			{
				Node tnode= XMLHandler.getSubNodeByNr(fields,"field",k);
				fieldNames[k] = XMLHandler.getTagValue(tnode,"name");
				keyFlags[k] = "Y".equalsIgnoreCase(XMLHandler.getTagValue(tnode,"key"));
				jsonNames[k] = XMLHandler.getTagValue(tnode,"json_name");
			}
		}
		catch(Exception e)
		{
			throw new KettleXMLException(BaseMessages.getString(PKG, "MongoDbUpdateMeta.Exception.UnableToLoadStepInfo"), e); //$NON-NLS-1$
		}
	}
	private void allocate(int num)
	{
		this.fieldNames = new String[num];
		this.jsonNames = new String[num];
		this.keyFlags = new boolean[num];
	}
	public void setDefault()
	{
		hostname = "127.0.0.1"; //$NON-NLS-1$
		port = "27017"; //$NON-NLS-1$
		dbName = "db";  //$NON-NLS-1$
		collection = "collection";  //$NON-NLS-1$
		arrayFlag=false;//$NON-NLS-1$
		primaryQuery="{}";//$NON-NLS-1$
		containerObject="";//$NON-NLS-1$
		authenticationUser="";//$NON-NLS-1$
		authenticationPassword="";//$NON-NLS-1$
		insertIfNotPresent=false;//$NON-NLS-1$
		trimType=0;//$NON-NLS-1$
		allocate(0);
	}
	
	public String getXML()
	{
		StringBuffer retval = new StringBuffer(300);
		retval.append("    ").append(XMLHandler.addTagValue("hostname", hostname)); //$NON-NLS-1$ //$NON-NLS-2$
		retval.append("    ").append(XMLHandler.addTagValue("port", port)); //$NON-NLS-1$ //$NON-NLS-2$
		retval.append("    ").append(XMLHandler.addTagValue("db_name", dbName)); //$NON-NLS-1$ //$NON-NLS-2$
		retval.append("    ").append(XMLHandler.addTagValue("collection", collection)); //$NON-NLS-1$ //$NON-NLS-2$
		retval.append("    ").append(XMLHandler.addTagValue("array_flag",arrayFlag)); //$NON-NLS-1$ //$NON-NLS-2$
		retval.append("    ").append(XMLHandler.addTagValue("insert_flag",insertIfNotPresent)); //$NON-NLS-1$ //$NON-NLS-2$
		retval.append("    ").append(XMLHandler.addTagValue("primary_query",primaryQuery));
		retval.append("    ").append(XMLHandler.addTagValue("container_object",containerObject));
		retval.append("    ").append(XMLHandler.addTagValue("trim_type",trimType));
	        retval.append("     <fields>" + Const.CR);
	        for (int i = 0; i < fieldNames.length; i++)
	        {
	            retval.append("      <field>" + Const.CR);
        	    retval.append("        ").append(XMLHandler.addTagValue("name", fieldNames[i]));
		    retval.append("        ").append(XMLHandler.addTagValue("key",keyFlags[i]));
		    retval.append("        ").append(XMLHandler.addTagValue("json_name",jsonNames[i]));
	            retval.append("       </field>" + Const.CR);
	        }
	        retval.append("      </fields>" + Const.CR);
		retval.append("    ").append(XMLHandler.addTagValue("auth_user", authenticationUser));//$NON-NLS-1$ //$NON-NLS-2$
		retval.append("    ").append(XMLHandler.addTagValue("auth_password", Encr.encryptPasswordIfNotUsingVariables(authenticationPassword))); //$NON-NLS-1$ //$NON-NLS-2$
		
		return retval.toString();
	}
	
	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String,Counter> counters)
		throws KettleException
	{
		try
		{
			hostname      = rep.getStepAttributeString (id_step, "hostname"); //$NON-NLS-1$
			port          = rep.getStepAttributeString (id_step, "port"); //$NON-NLS-1$
			dbName        = rep.getStepAttributeString (id_step, "db_name"); //$NON-NLS-1$
			collection    = rep.getStepAttributeString (id_step, "collection"); //$NON-NLS-1$
			authenticationUser = rep.getStepAttributeString(id_step, "auth_user");
			authenticationPassword = Encr.decryptPasswordOptionallyEncrypted(rep.getStepAttributeString(id_step, "auth_password"));
			primaryQuery = rep.getStepAttributeString(id_step,"primary_query");//$NON-NLS-1$
			arrayFlag = rep.getStepAttributeBoolean(id_step,"array_flag");//$NON-NLS-1$
			insertIfNotPresent = rep.getStepAttributeBoolean(id_step,"insert_flag");//$NON-NLS-1$
			containerObject = rep.getStepAttributeString(id_step,"container_object");//$NON-NLS-1$
			trimType = (int)rep.getStepAttributeInteger(id_step,"trim_type");//$NON-NLS-1$

			int fNum = rep.countNrStepAttributes(id_step,"field_name");
			allocate(fNum);
			for (int k=0;k<fNum;k++)
			{
				fieldNames[k] = rep.getStepAttributeString(id_step,k,"field_name");
				keyFlags[k] = rep.getStepAttributeBoolean(id_step,k,"key_flag");
				jsonNames[k] = rep.getStepAttributeString(id_step,k,"json_name");
			}
				
		}
		catch(Exception e)
		{
			throw new KettleException(BaseMessages.getString(PKG, "MongoDbUpdateMeta.Exception.UnexpectedErrorWhileReadingStepInfo"), e); //$NON-NLS-1$
		}
	}

	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step)
		throws KettleException
	{
		try
		{
			rep.saveStepAttribute(id_transformation, id_step, "hostname", hostname); //$NON-NLS-1$
			rep.saveStepAttribute(id_transformation, id_step, "port", port); //$NON-NLS-1$
			rep.saveStepAttribute(id_transformation, id_step, "db_name", dbName); //$NON-NLS-1$
			rep.saveStepAttribute(id_transformation, id_step, "collection", collection); //$NON-NLS-1$
			rep.saveStepAttribute(id_transformation, id_step, "auth_user", authenticationUser);
			rep.saveStepAttribute(id_transformation, id_step, "auth_password", Encr.encryptPasswordIfNotUsingVariables(authenticationPassword));
			rep.saveStepAttribute(id_transformation,id_step, "primary_query",primaryQuery);
			rep.saveStepAttribute(id_transformation,id_step,"container_object",containerObject);
			rep.saveStepAttribute(id_transformation,id_step,"array_flag",arrayFlag);
			rep.saveStepAttribute(id_transformation,id_step,"insert_flag",insertIfNotPresent);
			rep.saveStepAttribute(id_transformation,id_step,"trim_type",trimType);
			for ( int k = 0; k < fieldNames.length; k++)
			{
				rep.saveStepAttribute(id_transformation,id_step,k,"field_name",fieldNames[k]);
				rep.saveStepAttribute(id_transformation,id_step,k,"key_flag",keyFlags[k]);
				rep.saveStepAttribute(id_transformation,id_step,k,"json_name",jsonNames[k]);
			}
		}
		catch(KettleException e)
		{
			throw new KettleException(BaseMessages.getString(PKG, "MongoDbUpdateMeta.Exception.UnableToSaveStepInfo")+id_step, e); //$NON-NLS-1$
		}
	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans)
	{
		return new MongoDbUpdate(stepMeta, stepDataInterface, cnr, tr, trans);
	}

	public StepDataInterface getStepData()
	{
		return new MongoDbUpdateData();
	}

  @Override
  public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
    // TODO add checks
  }

  /**
   * @return the hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname the hostname to set
   */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /**
   * @return the port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port the port to set
   */
  public void setPort(String port) {
    this.port = port;
  }

  /**
   * @return the dbName
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * @param dbName the dbName to set
   */
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  /**
   * @return the collection
   */
  public String getCollection() {
    return collection;
  }

  /**
   * @param collection the collection to set
   */
  public void setCollection(String collection) {
    this.collection = collection;
  }

  /**
  *	@return field names array
  **/
  public String[] getFieldNames()
  {
	return this.fieldNames;
  }

  /**
  *	@param names field names array
  **/
  public void setFieldNames(String names[])
  {
  	this.fieldNames = names;
  }
  /**
  *	@return primary query
  **/
  public String getPrimaryQuery()
  {
  	return this.primaryQuery;
  }

  /**
  *	@param pquery primary query
  **/
  public void setPrimaryQuery(String pquery)
  {
  	this.primaryQuery = pquery;
  }
  /**
  *	@return key flags array
  **/
  public boolean[] getKeyFieldFlags()
  {
	return this.keyFlags;
  }
  /**
  *	@param flags key flags array
  **/
  public void setKeyFieldFlags(boolean[] flags)
  {
  	this.keyFlags=flags;
  }
  /**
  *	@return json names array
  **/
  public String[] getJSONNames()
  {
  	return jsonNames;
  }
  /**
  *	@param names json names array
  **/
  public void setJSONNames(String[] names)
  {
  	this.jsonNames= names;
  }
  /**
  *	@return Container object name
  **/
  public String getContainerObjectName()
  {
  	return this.containerObject;
  }
  /**
  *	@param name container object name
  **/
  public void setContinerObjectName(String name)
  {
  	this.containerObject=name;
  }
  /**
  *	@return is array flag
  **/
  public boolean getArrayFlag()
  {
  	return this.arrayFlag;
  }
  /**
  *	@param flag is array flag
  **/
  public void setArrayFlag(boolean flag)
  {
  	this.arrayFlag=flag;
  }

  /**
   * @return the authenticationUser
   */
  public String getAuthenticationUser() {
    return authenticationUser;
  }

  /**
   * @param authenticationUser the authenticationUser to set
   */
  public void setAuthenticationUser(String authenticationUser) {
    this.authenticationUser = authenticationUser;
  }

  /**
   * @return the authenticationPassword
   */
  public String getAuthenticationPassword() {
    return authenticationPassword;
  }

  /**
   * @param authenticationPassword the authenticationPassword to set
   */
  public void setAuthenticationPassword(String authenticationPassword) {
    this.authenticationPassword = authenticationPassword;
  }
  /**
  *	@return	insert if not present
  **/
  public boolean getInsertIfNotPresentFlag()
  {
  	return insertIfNotPresent;
  }
  /**
  *	@param flag	insert if not present flag
  **/
  public void setInsertIfNotPresentFlag( boolean flag)
  {
  	this.insertIfNotPresent=flag;
  }

  /**
  *	@return trim type
  **/
  public int getTrimType()
  {
  	return trimType;  
  }

  /**
  *	@param type trim type
  **/
  public void setTrimType(int type)
  {
  	this.trimType=type;
  }
}
