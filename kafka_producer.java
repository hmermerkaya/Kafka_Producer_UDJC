import java.net.UnknownHostException;
//import util.properties packages
import java.util.Map.Entry;
import java.util.Properties;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.io.InputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.List;
//import simple producer packages
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import kafka.common.FailedToSendMessageException;
import  org.apache.kafka.common.config.ConfigException;
//import org.apache.kafka.common.serialization.StringSerializer;

class SecuritySetException extends Exception {
   public SecuritySetException(String msg){
      super(msg);
   }
}

Producer producer;
RowMetaInterface inputRowMeta;
Integer cnt_failed;
Integer cnt_succeded;
RowSet outputStream_1;
RowSet outputStream_2;
RowSet outputStream_3;

Object[] r1=null;
Object[] r2=null;

public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException, SecuritySetException
{

 

if (first) {
	cnt_failed=0;
	cnt_succeded=0;
	first = false;
if (getParameter("SECURITY").equals("on") ){
  String  jaasConfigFile =getParameter("JAASCONFIGFILE");
  System.setProperty("java.security.auth.login.config", jaasConfigFile);
  System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
 System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
  System.setProperty("sun.security.krb5.debug", "true");
} else if (getParameter("SECURITY").equals("off") || getParameter("SECURITY")==null  ){

}else {
 
throw new SecuritySetException("<<<<<<<<<<PLEASE SET THE SECURITY IN THE PARAMETERS!!! on/off>>>>>>>>>>>>>>>>>");

}
	// outputRow_2 = null;
	outputStream_1 = findOutputRowSet("file1");
	outputStream_2 = findOutputRowSet("file2");
    outputStream_3 = findOutputRowSet("log");
	RowSet infoStream = findInfoRowSet("Props");

	Object[] infoRow = null;
	infoRow= getRowFrom(infoStream);
	String[]infofields =infoStream.getRowMeta().getFieldNames(); 
    //logBasic(infofields[1]);
 	Properties  prop = new Properties();
    
    for (int i=0;i < infofields.length; i++){
	String value = get(Fields.Info,infofields[i]).getString(infoRow);
	 //logBasic(infofields[i]+" = "+value);
	if (value==null  || value.equals("(default)")) continue;
   //  logBasic(infofields[i]+" = "+value);
 // prop.put(infofields[i],value);
     if (getParameter("SECURITY").equals("off") || getParameter("SECURITY")==null){
     	if (infofields[i].contains("security") || infofields[i].contains("sasl")) continue; 
     	else prop.put(infofields[i],value);
    } else prop.put(infofields[i],value);
}
	producer = new KafkaProducer(prop);
 
 

/*
  Properties props = new Properties();
  InputStream input = null;

  try {
       String filepath=getParameter("PROPS_FILE"); 
		input = new FileInputStream(filepath);
		
		props.load(input);

		producer = new KafkaProducer(props);
       
	} catch (IOException ex) {
		ex.printStackTrace();
	} finally {
		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

*/

    


} //EndIF 

r1 = getRow();
	if (r1 == null) {
		//outputRow_2=createOutputRow(outputRow_2, data.outputRowMeta.size());


		putRowTo(data.outputRowMeta,r2,outputStream_1);
        putRowTo(data.outputRowMeta,r2,outputStream_3);
		setOutputDone();
		return false;
	} 




inputRowMeta=getInputRowMeta().clone();
String []fields=inputRowMeta.getFieldNames();


String message="";
//logBasic("hello begin");
for (int i = 0; i < fields.length; i++){
    String field_tmp=getParameter("MESSAGE_FIELD_NAME");
    if (field_tmp .equals(fields[i]) ) {
	// logBasic("Field " +fields[i]);
	//if (fields[i] != null && fields[i].length() > 0 ) continue; 
	String tmp = get(Fields.In,fields[i]).getString(r1);
	//logBasic("tmp " +tmp);
	message=message+tmp;
	}
}
///logBasic("hello end");
//message=message.substring(0,message.length()-2);
//logBasic("message "+message);





String topic=getParameter("TOPIC_NAME");

ProducerRecord  producerRecord = new ProducerRecord (topic, message, message);
try{
	producer.send(producerRecord);
	cnt_succeded++;
}catch (FailedToSendMessageException  e){
	e.printStackTrace();
	cnt_failed++;
}
//Object[] outputRow = createOutputRow(r, data.outputRowMeta.size());
r2 = r1;
r2=createOutputRow(r2, data.outputRowMeta.size());
get(Fields.Out,"Succeded").setValue(r2,Integer.toString(cnt_succeded) );
get(Fields.Out,"Failed").setValue(r2, Integer.toString(cnt_failed));


putRowTo(data.outputRowMeta,r2,outputStream_2);
//putRowTo(data.outputRowMeta,r2,outputStream_3);
// putRow(data.outputRowMeta, r2);
	
return true;
}



public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {



	if ( producer != null) {
			producer.close();
			producer = null;
	}
	super.stopRunning(smi, sdi);
}
public void dispose(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

//outputRow_2=createOutputRow(r, data.outputRowMeta.size());
//putRowTo(data.outputRowMeta,outputRow_2,outputStream);

//get(Fields.Out,"Succeded").setValue(outputRow,Integer.toString(cnt_succeded) );
//get(Fields.Out,"Failed").setValue(outputRow, Integer.toString(cnt_failed));

//logBasic("message "+ cnt_succeded);		
	if ( producer != null) {
			producer.close();
			producer = null;
	}
	super.dispose(smi, sdi);
}
