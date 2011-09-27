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

 
/*
 * Created on 18-mei-2003
 *
 */

package org.pentaho.di.ui.trans.steps.mongodbupdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.Map;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.mongodbupdate.MongoDbUpdateMeta;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;
import org.pentaho.di.core.row.ValueMetaInterface;



/*
*	TODO:
*		Add tooltip
*/

public class MongoDbUpdateDialog extends BaseStepDialog implements StepDialogInterface
{
	private static Class<?> PKG = MongoDbUpdateMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private TextVar wHostname;
  	private TextVar wPort;
	private TextVar wDbName;
	private TextVar wCollection;
	private TextVar	wPrimaryQuery;
	private TextVar wContainerObject;
	private Button wInsert;
	private Button wArray;
	private TableView wFields;
	private TextVar wAuthUser;
	private TextVar wAuthPass;
	private CCombo wTrimType;

	private CTabFolder   wTabFolder;
	private FormData     fdTabFolder;
	
	private CTabItem     wConnectionTab, wStructureTab, wFieldsTab;

	private ColumnInfo[] colinf; 
	private Map<String, Integer> inputFields;

	private MongoDbUpdateMeta input;

	public MongoDbUpdateDialog(Shell parent,  Object in, TransMeta tr, String sname)
	{
		super(parent, (BaseStepMeta)in, tr, sname);
		input=(MongoDbUpdateMeta)in; 
		inputFields =new HashMap<String, Integer>();
	}

	public String open()
	{
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
 		props.setLook(shell);
        setShellImage(shell, input);

		ModifyListener lsMod = new ModifyListener() 
		{
			public void modifyText(ModifyEvent e) 
			{
				input.setChanged();
			}
		};
		changed = input.hasChanged();

		FormLayout formLayout = new FormLayout ();
		formLayout.marginWidth  = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.Shell.Title")); //$NON-NLS-1$
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname=new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.Stepname.Label")); //$NON-NLS-1$
 		props.setLook(wlStepname);
		fdlStepname=new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right= new FormAttachment(middle, -margin);
		fdlStepname.top  = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname=new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
 		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname=new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top  = new FormAttachment(0, margin);
		fdStepname.right= new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);
		Control lastControl = wStepname; 

		wTabFolder = new CTabFolder(shell,SWT.BORDER);
		props.setLook(wTabFolder,Props.WIDGET_STYLE_TAB);


		///////////////////////////////////
		//        CONNECTION TAB         //
		///////////////////////////////////

		wConnectionTab = new CTabItem(wTabFolder,SWT.NONE);
		wConnectionTab.setText(BaseMessages.getString(PKG,"MongoDbUpdateDialog.ConnectionTab.TabLabel"));
		Composite wConnectionComp = new Composite(wTabFolder,SWT.NONE);
		props.setLook(wConnectionComp);
		
		FormLayout connectionLayout = new FormLayout();
		connectionLayout.marginWidth=Const.FORM_MARGIN;
		connectionLayout.marginHeight=Const.FORM_MARGIN;
		wConnectionComp.setLayout(connectionLayout);

		// Hostname input ...
		//
		Label wlHostname = new Label(wConnectionComp, SWT.RIGHT);
		wlHostname.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.Hostname.Label")); //$NON-NLS-1$
 		props.setLook(wlHostname);
		FormData fdlHostname = new FormData();
		fdlHostname.left = new FormAttachment(0, 0);
		fdlHostname.right= new FormAttachment(middle, -margin);
		fdlHostname.top  = new FormAttachment(lastControl, margin);
		wlHostname.setLayoutData(fdlHostname);
		wHostname=new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wHostname);
		wHostname.addModifyListener(lsMod);
		FormData fdHostname = new FormData();
		fdHostname.left = new FormAttachment(middle, 0);
		fdHostname.top  = new FormAttachment(lastControl, margin);
		fdHostname.right= new FormAttachment(100, 0);
		wHostname.setLayoutData(fdHostname);
		lastControl = wHostname;
		// Port input ...
		//
		Label wlPort = new Label(wConnectionComp, SWT.RIGHT);
		wlPort.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.Port.Label")); //$NON-NLS-1$
		props.setLook(wlPort);
		FormData fdlPort = new FormData();
		fdlPort.left = new FormAttachment(0, 0);
		fdlPort.right= new FormAttachment(middle, -margin);
		fdlPort.top  = new FormAttachment(lastControl, margin);
		wlPort.setLayoutData(fdlPort);
		wPort=new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wPort);
		wPort.addModifyListener(lsMod);
		FormData fdPort = new FormData();
		fdPort.left = new FormAttachment(middle, 0);
		fdPort.top  = new FormAttachment(lastControl, margin);
		fdPort.right= new FormAttachment(100, 0);
		wPort.setLayoutData(fdPort);
		lastControl = wPort;
		// DbName input ...
		//
		Label wlDbName = new Label(wConnectionComp, SWT.RIGHT);
		wlDbName.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.DbName.Label")); //$NON-NLS-1$
		props.setLook(wlDbName);
		FormData fdlDbName = new FormData();
		fdlDbName.left = new FormAttachment(0, 0);
		fdlDbName.right= new FormAttachment(middle, -margin);
		fdlDbName.top  = new FormAttachment(lastControl, margin);
		wlDbName.setLayoutData(fdlDbName);
		wDbName=new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wDbName);
		wDbName.addModifyListener(lsMod);
		FormData fdDbName = new FormData();
		fdDbName.left = new FormAttachment(middle, 0);
		fdDbName.top  = new FormAttachment(lastControl, margin);
		fdDbName.right= new FormAttachment(100, 0);
		wDbName.setLayoutData(fdDbName);
		lastControl = wDbName;
		// Collection input ...
		//
		Label wlCollection = new Label(wConnectionComp, SWT.RIGHT);
		wlCollection.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.Collection.Label")); //$NON-NLS-1$
		props.setLook(wlCollection);
		FormData fdlCollection = new FormData();
		fdlCollection.left = new FormAttachment(0, 0);
		fdlCollection.right= new FormAttachment(middle, -margin);
		fdlCollection.top  = new FormAttachment(lastControl, margin);
		wlCollection.setLayoutData(fdlCollection);
		wCollection=new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wCollection);
		wCollection.addModifyListener(lsMod);
		FormData fdCollection = new FormData();
		fdCollection.left = new FormAttachment(middle, 0);
		fdCollection.top  = new FormAttachment(lastControl, margin);
		fdCollection.right= new FormAttachment(100, 0);
		wCollection.setLayoutData(fdCollection);
		lastControl = wCollection;
		// Authentication...
		// AuthUser line
		Label wlAuthUser = new Label(wConnectionComp, SWT.RIGHT);
		wlAuthUser.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.AuthenticationUser.Label"));
		props.setLook(wlAuthUser);
		FormData fdlAuthUser = new FormData();
		fdlAuthUser.left = new FormAttachment(0, -margin);
		fdlAuthUser.top = new FormAttachment(lastControl, margin);
		fdlAuthUser.right = new FormAttachment(middle, -margin);
		wlAuthUser.setLayoutData(fdlAuthUser);
		wAuthUser = new TextVar(transMeta, wConnectionComp, SWT.BORDER | SWT.READ_ONLY);
		wAuthUser.setEditable(true);
		props.setLook(wAuthUser);
		wAuthUser.addModifyListener(lsMod);
		FormData fdAuthUser = new FormData();
		fdAuthUser.left = new FormAttachment(middle, 0);
		fdAuthUser.top = new FormAttachment(lastControl, margin);
		fdAuthUser.right = new FormAttachment(100, 0);
		wAuthUser.setLayoutData(fdAuthUser);
		lastControl = wAuthUser;
		// AuthPass line
		Label wlAuthPass = new Label(wConnectionComp, SWT.RIGHT);
		wlAuthPass.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.AuthenticationPassword.Label"));
		props.setLook(wlAuthPass);
		FormData fdlAuthPass = new FormData();
		fdlAuthPass.left = new FormAttachment(0, -margin);
		fdlAuthPass.top = new FormAttachment(lastControl, margin);
		fdlAuthPass.right = new FormAttachment(middle, -margin);
		wlAuthPass.setLayoutData(fdlAuthPass);
		wAuthPass = new TextVar(transMeta, wConnectionComp, SWT.BORDER | SWT.READ_ONLY);
		wAuthPass.setEditable(true);
		wAuthPass.setEchoChar('*');
		props.setLook(wAuthPass);
		wAuthPass.addModifyListener(lsMod);
		FormData fdAuthPass = new FormData();
		fdAuthPass.left = new FormAttachment(middle, 0);
		fdAuthPass.top = new FormAttachment(lastControl, margin);
		fdAuthPass.right = new FormAttachment(100, 0);
		wAuthPass.setLayoutData(fdAuthPass);
		lastControl = wAuthPass;

		FormData fdConnectionComp = new FormData();
		fdConnectionComp.left = new FormAttachment(0,0);
		fdConnectionComp.top = new FormAttachment(0,0);
		fdConnectionComp.right = new FormAttachment(100,0);
		fdConnectionComp.bottom = new FormAttachment(100,0);
		wConnectionComp.setLayoutData(fdConnectionComp);

		wConnectionComp.layout();
		wConnectionTab.setControl(wConnectionComp);

		///////////////////////////////////
		//         STRUCTURE TAB         //
		///////////////////////////////////

		wStructureTab = new CTabItem(wTabFolder,SWT.NONE);
		wStructureTab.setText(BaseMessages.getString(PKG,"MongoDbUpdateDialog.StructureTab.TabLabel"));
		Composite wStructureComp = new Composite(wTabFolder,SWT.NONE);
		props.setLook(wStructureComp);
		
		FormLayout structureLayout = new FormLayout();
		structureLayout.marginWidth=Const.FORM_MARGIN;
		structureLayout.marginHeight=Const.FORM_MARGIN;
		wStructureComp.setLayout(structureLayout);
		
		// Primary Query input ...
		//
		Label wlPrimaryQuery = new Label(wStructureComp, SWT.RIGHT);
		wlPrimaryQuery.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.PrimaryQuery.Label")); //$NON-NLS-1$
 		props.setLook(wlPrimaryQuery);
		FormData fdlPrimaryQuery = new FormData();
		fdlPrimaryQuery.left = new FormAttachment(0, 0);
		fdlPrimaryQuery.right= new FormAttachment(middle, -margin);
		fdlPrimaryQuery.top  = new FormAttachment(0, margin);
		wlPrimaryQuery.setLayoutData(fdlPrimaryQuery);
		wPrimaryQuery=new TextVar(transMeta, wStructureComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wPrimaryQuery);
		wPrimaryQuery.addModifyListener(lsMod);
		FormData fdPrimaryQuery = new FormData();
		fdPrimaryQuery.left = new FormAttachment(middle, 0);
		fdPrimaryQuery.top  = new FormAttachment(lastControl, margin);
		fdPrimaryQuery.right= new FormAttachment(100, 0);
		wPrimaryQuery.setLayoutData(fdPrimaryQuery);
		lastControl = wPrimaryQuery;
		// Container Object input ...
		//
		Label wlContainerObject = new Label(wStructureComp, SWT.RIGHT);
		wlContainerObject.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.ContainerObject.Label")); //$NON-NLS-1$
 		props.setLook(wlContainerObject);
		FormData fdlContainerObject = new FormData();
		fdlContainerObject.left = new FormAttachment(0, 0);
		fdlContainerObject.right= new FormAttachment(middle, -margin);
		fdlContainerObject.top  = new FormAttachment(lastControl, margin);
		wlContainerObject.setLayoutData(fdlContainerObject);
		wContainerObject=new TextVar(transMeta, wStructureComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wContainerObject);
		wContainerObject.addModifyListener(lsMod);
		FormData fdContainerObject = new FormData();
		fdContainerObject.left = new FormAttachment(middle, 0);
		fdContainerObject.top  = new FormAttachment(lastControl, margin);
		fdContainerObject.right= new FormAttachment(100, 0);
		wContainerObject.setLayoutData(fdContainerObject);
		lastControl = wContainerObject;
		// Array check input ...
		//
		Label wlArray = new Label(wStructureComp, SWT.RIGHT);
		wlArray.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.Array.Label")); //$NON-NLS-1$
 		props.setLook(wlArray);
		FormData fdlArray = new FormData();
		fdlArray.left = new FormAttachment(0, 0);
		fdlArray.right= new FormAttachment(middle, -margin);
		fdlArray.top  = new FormAttachment(lastControl, margin);
		wlArray.setLayoutData(fdlArray);
		wArray=new Button(wStructureComp, SWT.CHECK);
 		props.setLook(wArray);
		FormData fdArray = new FormData();
		fdArray.left = new FormAttachment(middle, 0);
		fdArray.top  = new FormAttachment(lastControl, margin);
		fdArray.right= new FormAttachment(100, 0);
		wArray.setLayoutData(fdArray);
		lastControl = wArray;
		// Insert check input ...
		//
		Label wlInsert = new Label(wStructureComp, SWT.RIGHT);
		wlInsert.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.Insert.Label")); //$NON-NLS-1$
 		props.setLook(wlInsert);
		FormData fdlInsert = new FormData();
		fdlInsert.left = new FormAttachment(0, 0);
		fdlInsert.right= new FormAttachment(middle, -margin);
		fdlInsert.top  = new FormAttachment(lastControl, margin);
		wlInsert.setLayoutData(fdlInsert);
		wInsert=new Button(wStructureComp, SWT.CHECK);
 		props.setLook(wInsert);
		FormData fdInsert = new FormData();
		fdInsert.left = new FormAttachment(middle, 0);
		fdInsert.top  = new FormAttachment(lastControl, margin);
		fdInsert.right= new FormAttachment(100, 0);
		wInsert.setLayoutData(fdInsert);
		lastControl = wInsert;

		Label wlTrimType = new Label(wStructureComp, SWT.RIGHT);
		wlTrimType.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.TrimType.Label")); //$NON-NLS-1$
 		props.setLook(wlTrimType);
		FormData fdlTrim = new FormData();
		fdlTrim.left = new FormAttachment(0, 0);
		fdlTrim.right= new FormAttachment(middle, -margin);
		fdlTrim.top  = new FormAttachment(lastControl, margin);
		wlTrimType.setLayoutData(fdlTrim);

		wTrimType = new CCombo(wStructureComp,SWT.BORDER | SWT.READ_ONLY);
		wTrimType.setToolTipText(BaseMessages.getString(PKG,"MongoDbUpdateDialog.TrimType.Tooltip")); //$NON-NLS-1$
		wTrimType.setEditable(true);
		wTrimType.setItems(MongoDbUpdateMeta.TrimTypes);
		wTrimType.addModifyListener(lsMod);
 		props.setLook(wTrimType);
		FormData fdTrimType = new FormData();
		fdTrimType.left = new FormAttachment(middle, 0);
		fdTrimType.top  = new FormAttachment(lastControl, margin);
		fdTrimType.right= new FormAttachment(100, 0);
		wTrimType.setLayoutData(fdTrimType);
		lastControl = wTrimType;

		FormData fdStructureComp = new FormData();
		fdStructureComp.left = new FormAttachment(0,0);
		fdStructureComp.top = new FormAttachment(0,0);
		fdStructureComp.right = new FormAttachment(100,0);
		fdStructureComp.bottom = new FormAttachment(100,0);
		wStructureComp.setLayoutData(fdStructureComp);

		wStructureComp.layout();
		wStructureTab.setControl(wStructureComp);


		///////////////////////////////////
		//          FIELDS TAB           //
		///////////////////////////////////

		// Fields tab...
		//
		wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
		wFieldsTab.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.FieldsTab.TabLabel"));
		
		FormLayout fieldsLayout = new FormLayout ();
		fieldsLayout.marginWidth  = Const.FORM_MARGIN;
		fieldsLayout.marginHeight = Const.FORM_MARGIN;
		
		Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
		wFieldsComp.setLayout(fieldsLayout);
 		props.setLook(wFieldsComp);

		wGet=new Button(wFieldsComp, SWT.PUSH);
		wGet.setText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.Get.Button"));
		wGet.setToolTipText(BaseMessages.getString(PKG, "MongoDbUpdateDialog.Get.Tooltip"));

		setButtonPositions(new Button[] { wGet }, margin, null);

		final int FieldsRows=input.getFieldNames().length;
		colinf=new ColumnInfo[]
		{
			new ColumnInfo(
					BaseMessages.getString(PKG, "MongoDbUpdateDialog.FieldNameColumn.Column"),
					ColumnInfo.COLUMN_TYPE_CCOMBO, 
					new String[] { "" }, 
					false
			),
			new ColumnInfo(
					BaseMessages.getString(PKG, "MongoDbUpdateDialog.KeyColumn.Column"),
					ColumnInfo.COLUMN_TYPE_CCOMBO, 
					new String[] { BaseMessages.getString(PKG, "System.Combo.No"), BaseMessages.getString(PKG, "System.Combo.Yes") }
			),
    			new ColumnInfo(
					BaseMessages.getString(PKG, "MongoDbUpdateDialog.JSONNameColumn.Column"),
					ColumnInfo.COLUMN_TYPE_TEXT,   
					false
			)
		};
		
		wFields=new TableView(transMeta, wFieldsComp, 
						      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, 
						      colinf, 
						      FieldsRows,  
						      lsMod,
						      props
						      );

		FormData fdFields=new FormData();
		fdFields.left  = new FormAttachment(0, 0);
		fdFields.top   = new FormAttachment(0, 0);
		fdFields.right = new FormAttachment(100, 0);
		fdFields.bottom = new FormAttachment(wGet,-margin);
		wFields.setLayoutData(fdFields);

		// 
		// Search the fields in the background
			
		final Runnable runnable = new Runnable()
		{
		    public void run()
		    {
			StepMeta stepMeta = transMeta.findStep(stepname);
			if (stepMeta!=null)
			{
			    try
			    {
				RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);
			       
				// Remember these fields...
				for (int i=0;i<row.size();i++)
				{
				    inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
				}
				setComboBoxes();
			    }
			    catch(KettleException e)
			    {
				logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
			    }
			}
		    }
		};
	        new Thread(runnable).start();
		
		FormData fdFieldsComp=new FormData();
		fdFieldsComp.left  = new FormAttachment(0, 0);
		fdFieldsComp.top   = new FormAttachment(0, 0);
		fdFieldsComp.right = new FormAttachment(100, 0);
		fdFieldsComp.bottom= new FormAttachment(100, 0);
		wFieldsComp.setLayoutData(fdFieldsComp);
		
		wFieldsComp.layout();
		wFieldsTab.setControl(wFieldsComp);
		
		fdTabFolder = new FormData();
		fdTabFolder.left  = new FormAttachment(0, 0);
		fdTabFolder.top   = new FormAttachment(wStepname, margin);
		fdTabFolder.right = new FormAttachment(100, 0);
		fdTabFolder.bottom= new FormAttachment(100, -50);
		wTabFolder.setLayoutData(fdTabFolder);
		wTabFolder.setSelection(0);
	

		// Some buttons
		wOK=new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$
   		wCancel=new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$

		setButtonPositions(new Button[] { wOK, wCancel}, margin, wTabFolder);

		// Add listeners
		lsCancel   = new Listener() { public void handleEvent(Event e) { cancel(); } };
		lsOK       = new Listener() { public void handleEvent(Event e) { ok();     } };
		lsGet      = new Listener() { public void handleEvent(Event e) { get();      } };
		
		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener    (SWT.Selection, lsOK    );
		wGet.addListener   (SWT.Selection, lsGet   );
		
		lsDef=new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };
		
		wStepname.addSelectionListener( lsDef );
		wHostname.addSelectionListener( lsDef );
		
		
		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(	new ShellAdapter() { public void shellClosed(ShellEvent e) { cancel(); } } );
		
		getData();
		input.setChanged(changed);

		// Set the shell size, based upon previous time...
		setSize();
		
		shell.open();
		while (!shell.isDisposed())
		{
				if (!display.readAndDispatch()) display.sleep();
		}
		return stepname;
	}
	protected void setComboBoxes()
    	{
        	// Something was changed in the row.
	        //
        	final Map<String, Integer> fields = new HashMap<String, Integer>();
		        
	        // Add the currentMeta fields...
	        fields.putAll(inputFields);
	        
	        Set<String> keySet = fields.keySet();
	        List<String> entries = new ArrayList<String>(keySet);
	
	        String fieldNames[] = (String[]) entries.toArray(new String[entries.size()]);
	
	        Const.sortStrings(fieldNames);
	        colinf[0].setComboValues(fieldNames);
	}	 
	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */ 
	public void getData()
	{
		wHostname.setText(Const.NVL(input.getHostname(), "")); //$NON-NLS-1$
    		wPort.setText(Const.NVL(input.getPort(), "")); //$NON-NLS-1$
		wDbName.setText(Const.NVL(input.getDbName(), "")); //$NON-NLS-1$
		wCollection.setText(Const.NVL(input.getCollection(), "")); //$NON-NLS-1$
		wPrimaryQuery.setText(Const.NVL(input.getPrimaryQuery(),"")); //$NON-NLS-1$
		wContainerObject.setText(Const.NVL(input.getContainerObjectName(),"")); //$NON-NLS-1$
		wArray.setSelection(input.getArrayFlag());
		wInsert.setSelection(input.getInsertIfNotPresentFlag());
		wTrimType.select(input.getTrimType());
		for (int k=0;k<input.getFieldNames().length;k++)
		{
			TableItem item = wFields.table.getItem(k);
			item.setText(1,input.getFieldNames()[k]);
			item.setText(3,input.getJSONNames()[k]);
			item.setText(2,input.getKeyFieldFlags()[k]?BaseMessages.getString(PKG, "System.Combo.Yes"):BaseMessages.getString(PKG, "System.Combo.No"));
		}

		wAuthUser.setText(Const.NVL(input.getAuthenticationUser(), "")); // $NON-NLS-1$
		wAuthPass.setText(Const.NVL(input.getAuthenticationPassword(), "")); // $NON-NLS-1$

		wStepname.selectAll();
	}
	
	private void cancel()
	{
		stepname=null;
		input.setChanged(changed);
		dispose();
	}

  	private void getInfo(MongoDbUpdateMeta meta) 
	{
	    meta.setHostname(Const.NVL(wHostname.getText(),""));
	    meta.setPort(Const.NVL(wPort.getText(),""));
	    meta.setDbName(Const.NVL(wDbName.getText(),""));
	    meta.setCollection(Const.NVL(wCollection.getText(),""));
	    meta.setPrimaryQuery(Const.NVL(wPrimaryQuery.getText(),""));
	    meta.setContinerObjectName(Const.NVL(wContainerObject.getText(),""));
	    meta.setArrayFlag(wArray.getSelection());
	    meta.setInsertIfNotPresentFlag(wInsert.getSelection());
	    int intTrim = wTrimType.getSelectionIndex();
	    if ((intTrim<0)||(intTrim>=MongoDbUpdateMeta.TrimTypes.length)) intTrim=0;
	    meta.setTrimType(intTrim);
	    
	    int nrRow = wFields.nrNonEmpty();
	    String[] fNames = new String[nrRow];
	    String[] jNames = new String[nrRow];
	    boolean[] kFlags = new boolean[nrRow];
	    for (int k=0;k<nrRow;k++)
	    {
	    	TableItem item=wFields.getNonEmpty(k);
		fNames[k]=Const.NVL(item.getText(1),"");
		jNames[k]=(item.getText(3).length()>0)?Const.NVL(item.getText(3),""):Const.NVL(item.getText(1),"");
		kFlags[k] = BaseMessages.getString(PKG,"System.Combo.Yes").equalsIgnoreCase(item.getText(2));
	    }

	    meta.setFieldNames(fNames);
	    meta.setJSONNames(jNames);
	    meta.setKeyFieldFlags(kFlags);
	    meta.setAuthenticationUser(Const.NVL(wAuthUser.getText(),""));
	    meta.setAuthenticationPassword(Const.NVL(wAuthPass.getText(),""));
	}

	private void ok()
	{
		if (Const.isEmpty(wStepname.getText())) return;

		stepname = wStepname.getText(); // return value

		getInfo(input);
    
		dispose();
	}

	private void get()
	{	
		try
		{
			RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			if (r!=null && !r.isEmpty())
			{
		                TableItemInsertListener listener = new TableItemInsertListener()
				{
		                        public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v)
		                        {
		                            tableItem.setText(2,BaseMessages.getString(PKG, "System.Combo.No"));
					    tableItem.setText(3,tableItem.getText(1));
		                            return true;
		                        }
                		};
		                BaseStepDialog.getFieldsFromPrevious(r, wFields, 1, new int[] { 1 }, new int[] {}, 0, 0, listener);
			}
		}
		catch(KettleException ke)
		{
			new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"), BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"), ke);
		}

	}

}
