/**
 * Licensed Materials - Property of IBM
 *
 * (C) Copyright IBM Corp. 2013 All Rights Reserved.
 *
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with
 * IBM Corp.
 */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.TreeMap;

public class ChinaSellIn_all_w_service_inbilling {

	private Connection m_dbConnection = null;
	private BufferedWriter m_Writer = null;
	private Map<String, String> m_mapOfArgs = null;
	private long m_lCount = 0l;
	private String m_sIwTs = null;
	private String m_sIwTs_end = null;
	private NumberFormat m_nfUS = NumberFormat.getNumberInstance(Locale.US);
	private Map<String,String> m_mapOfUsedSN = null;
	private String USEDSNMAP_FILENAME = "BHCNIBMSI.map";
	private final String _csv_endofline = "\r\n";
	private final char _csv_separator = ','; //'\t'

	
	public static void main(String[] args) throws Exception {
		/* this shall come from the command line */
		Map<String, String> mapOfArgs = new HashMap<String, String>();
		mapOfArgs.put("DB.NAME", "EUBADB2A");
		mapOfArgs.put("DB.HOST", "meuba.vipa.uk.ibm.com");
		mapOfArgs.put("DB.PORT", "446");
		mapOfArgs.put("DB.USER", "CN74283");
		mapOfArgs.put("DB.PASSWORD", "6yhn6yhn");
		//full sell-in
		mapOfArgs.put("PATCH", "XTRANSTYPE, +BILL-, MICXT");
		mapOfArgs.put("CACHEFILE", "SELLIN.cache");
		mapOfArgs.put("STARTTACTIONTS", "2017-04-02 00:00:00.000000");
		mapOfArgs.put("ENDTACTIONTS", "2017-06-30 00:00:00.000000");
		mapOfArgs.put("COUNTER", "0");
		
		(new ChinaSellIn_all_w_service_inbilling()).createReport(mapOfArgs);
		
	}
	
	public void createReport(Map<String, String> mapOfArgs) {
		
		try {
			runQuery(mapOfArgs);
			
			System.out.println("last counter: " + m_lCount);
			System.out.println("last timestamp: " + m_sIwTs);

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeWriter();
			closeDb();
		}
	
		return;
	}
	
	
	private void runQuery(Map<String, String> mapOfArgs) throws Exception {
		m_mapOfArgs = mapOfArgs;
		m_sIwTs = m_mapOfArgs.get("STARTTACTIONTS");
		m_sIwTs_end = m_mapOfArgs.get("ENDTACTIONTS");
		m_lCount = Long.parseLong(m_mapOfArgs.get("COUNTER"));
		
		String sLastBillDate = null;
		String sEndBillDate = null;
		try {
			Calendar calBD = Calendar.getInstance(TimeZone.getTimeZone("Europe/Paris"));
			SimpleDateFormat sdfBD = new SimpleDateFormat("yyyy-MM-dd");
			calBD.setTime(sdfBD.parse(m_sIwTs));
			calBD.add(java.util.Calendar.DAY_OF_MONTH,0);
			sLastBillDate = sdfBD.format(calBD.getTime());
			
			calBD.setTime(sdfBD.parse(m_sIwTs_end));
			calBD.add(java.util.Calendar.DAY_OF_MONTH,0);
			sEndBillDate = sdfBD.format(calBD.getTime());
		} catch (Exception e) {}
		
		String sSalesOrg = "'0222','0684'";
		//String sCustome = "'BP10AILL33','BP00088SUT','BP18Z7GDE6','BP0006A20I','BP0007GQO6','BP0007GW87','BP0ZTZJB73'";

//Invoice data, if order reason = 'BLB'...	
		String sQuery = "SELECT DISTINCT T2BILL.SALES_DOC_ORDER_NO, "
				+ "RTRIM(T2BILL.LEGAL_CONTRCT_CPS_NO) AS LEGAL_CONTRCT_CPS_NO, "
				+ "RTRIM(T2BILL.ORDER_REASON) AS ORDER_REASON " +
		"FROM WSDIW.CHSW_WW_BILL T2BILL " +
		"WHERE (T2BILL.SALES_ORG IN (" + sSalesOrg + ")) " +
		//"AND (T2BILL.SALES_GRP IN ('04','14')) " +
		"AND (T2BILL.CUSTNO_SOLD_TO LIKE 'BP%' OR T2BILL.CUSTNO_BILL_TO LIKE 'BP%' OR T2BILL.CUSTNO_PAYER LIKE 'BP%' OR T2BILL.CUSTNO_SHIP_TO LIKE 'BP%') " + 
		//"AND (T2BILL.CUSTNO_SOLD_TO = 'BP0008I842' OR T2BILL.CUSTNO_BILL_TO = 'BP0008I842' OR T2BILL.CUSTNO_PAYER = 'BP0008I842' OR T2BILL.CUSTNO_SHIP_TO = 'BP0008I842') " +
		"AND (T2BILL.BILL_TYPE_CD IN ('ZF2','ZS1','ZF2I','ZS1I')) "+
		//"AND  (T2BILL.CUSTNO_SOLD_TO IN (" + sCustome + ") OR  T2BILL.CUSTNO_BILL_TO IN (" + sCustome + ") OR  T2BILL.CUSTNO_PAYER IN (" + sCustome + ") OR  T2BILL.CUSTNO_SHIP_TO IN (" + sCustome + ") ) "+
		"AND (T2BILL.IW_VALID_FROM_TS > TIMESTAMP('" + m_sIwTs + "') " +
		"AND T2BILL.IW_VALID_FROM_TS < TIMESTAMP('" + m_sIwTs_end + "')) " +
		//"AND (T2BILL.BILL_DOC_NO = '8901123471' ) " +
		//"AND (T2BILL.LEGAL_CONTRCT_CPS_NO IN ('8300127333',	'8300128878',	'8300128974',	'8300129876',	'8300131313',	'8300131556',	'8300131177',	'8300131270',	'8300133373',	'8300132206',	'8300136512',	'8300136529',	'8300136375',	'8300136379',	'8300136372',	'8300139281',	'8300139471',	'8300138790',	'8300140305',	'8300143663',	'8300143090',	'8300143819',	'8300144376',	'8300141340',	'8300144750',	'8300144567',	'8300145499',	'8300145972',	'8300146285',	'8300149372',	'8300149246',	'8300144252',	'8300150921')) " +
		//"AND (T2BILL.LEGAL_CONTRCT_CPS_NO IN ('8300145771','8300145811','8300147063')) " +
		//"AND (T2BILL.SALES_DOC_ORDER_NO IN ('7200135923')) " +
		"FOR READ ONLY ";

		Map<String, String> mapOfCPSNo = new TreeMap<String, String>();
		Map<String, String> mapOfBLBCPSNo = new TreeMap<String, String>();
		Map<String, String> mapOfOrderNo = new TreeMap<String, String>();

		ResultSet rs = getDbStatement(false).executeQuery(sQuery);
		ResultSetMetaData rsmd = rs.getMetaData();
	    int iColCount = rsmd.getColumnCount();
		String[] ar_sField = null;
	    int iCount = 0;
		while (rs.next()) {
			iCount++;
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    	if (ar_sField[r-1]!= null) {
		    		ar_sField[r-1] = ar_sField[r-1].trim();
		    	}
		    }
		    if ((ar_sField[2]!=null) && (ar_sField[2].equals("BLB"))) {
		    	if ((ar_sField[1] != null) && (ar_sField[1].length()>0)) {
		    		mapOfBLBCPSNo.put(ar_sField[1], ar_sField[0]); // only last order# kept in the map
		    	}
		    	mapOfOrderNo.put(ar_sField[0], ar_sField[1]);
		    } else {
		    	if ((ar_sField[1] != null) && (ar_sField[1].length()>0)) {
		    		mapOfCPSNo.put(ar_sField[1], ar_sField[0]);
		    	}
		    	mapOfOrderNo.put(ar_sField[0], ar_sField[1]);
		    }
		}

		rs.close();
		if ((iCount == 0) || (mapOfOrderNo.size() == 0)) {
			return;
		}

		StringBuilder sBufOrderNo = null;
		Iterator<String> iter = null;
	    String sTmp = null;
		sBufOrderNo = new StringBuilder((mapOfOrderNo.size()+1)*13);
		iter = mapOfOrderNo.keySet().iterator();
		sBufOrderNo.append('(');
		while (iter.hasNext()) {
			sBufOrderNo.append('\'');
			sBufOrderNo.append(iter.next());
			sBufOrderNo.append("',");
		}
		sBufOrderNo.setCharAt(sBufOrderNo.length()-1, ')');

		StringBuilder sBufBLBCPSNo = new StringBuilder((mapOfBLBCPSNo.size()+1)*13);
		if (mapOfBLBCPSNo.size() > 0) {
			iter = mapOfBLBCPSNo.keySet().iterator();
			sBufBLBCPSNo.append('(');
			while (iter.hasNext()) {
				sBufBLBCPSNo.append('\'');
				sBufBLBCPSNo.append(iter.next());
				sBufBLBCPSNo.append("',");
			}
			sBufBLBCPSNo.setCharAt(sBufBLBCPSNo.length()-1, ')');
		}		
		
		sQuery = "SELECT DISTINCT HDER.SALES_DOC_ORDER_NO, "
				+ "HDER.SALES_DOC_DATE, "
				+ "RTRIM(HDER.LEGAL_CONTRCT_CPS_NO) AS LEGAL_CONTRCT_CPS_NO, " 
				+ "RTRIM(HDER.SALES_DOC_TYPE) AS SALES_DOC_TYPE, "
				+ "RTRIM(HDER.ORDER_REASON) AS ORDER_REASON, "
				+ "RTRIM(HDER.SALES_GRP) AS SALES_GROUP, "
				+ "RTRIM(QUOTE_NO_CRM) AS QUOTE_NO_CRM, " 
				+  "RTRIM(HDER.CHANL_ROLE_HEAD) AS CHANL_ROLE, "
				+ "RTRIM(HDER.SUBMT_ROLE) AS SUBMT_ROLE, "
				+ "RTRIM(QUOTE_NO_ECC) AS QUOTE_NO_ECC, " 
				+ "RTRIM(SPEC_BID_INDC), "
				+ "RTRIM(SPEC_BID_INDC_SPH) " +
				"FROM WSDIW.CHSW_WW_ORD_HEADER HDER " +
				"WHERE (HDER.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ") " + 
                "AND (HDER.SALES_DOC_TYPE IN ('ZGOR','ZBPS','ZNPC')) " +
                //"AND (HDER.CHANL_ROLE_HEAD ='18' OR HDER.ORDER_REASON = 'B48' ) " +
                //"AND (HDER.CHANL_ROLE_HEAD NOT IN ('18')) " +
    			"AND (HDER.ORDER_REASON <> 'B48') " +   //MWC order reason code
                //"AND (HDER.SALES_DOC_ORDER_NO = '7200098320') " +
				"FOR READ ONLY "; 
				
		mapOfOrderNo.clear();
		List<String[]> listOfOrder = new ArrayList<String[]>();
		Map<String, String> mapOfQuote = new TreeMap<String, String>();
		Map<String, String> mapOfCPSQuoteNo = new TreeMap<String, String>();
		Map<String, String[]> mapOfCPSQuoteItem = new HashMap<String, String[]>();
		Map<String, String> mapOfSrvContractNo = new TreeMap<String, String>();
		
		rs = getDbStatement(false).executeQuery(sQuery);
		rsmd = rs.getMetaData();
	    iColCount = rsmd.getColumnCount();
	    iCount = 0;
	    sTmp = null;
		while (rs.next()) {
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    }
		    mapOfOrderNo.put(ar_sField[0], String.valueOf(iCount));
		    listOfOrder.add(ar_sField);
		    
		    if ((ar_sField[6]!=null) && (ar_sField[6].trim().length()>0)) {
		    	mapOfQuote.put(ar_sField[6].trim(), ar_sField[0]);
		    } 
		    else{
		    	if ((ar_sField[9]!=null) && (ar_sField[9].trim().length()>0)) {
			    	mapOfQuote.put(ar_sField[9].trim(), ar_sField[0]);
			    } 
		    }
		    
	    	if ((ar_sField[3]!=null) && ("ZNPC".equals(ar_sField[3]))) {
		    	mapOfSrvContractNo.put(ar_sField[0], null);
			    if ((ar_sField[2]!=null) && (ar_sField[2].trim().length()>0)) {
				    mapOfCPSQuoteNo.put(ar_sField[2], null);
			    } 
		    }
			iCount++;
		}
		rs.close();
		
		if ((iCount == 0) || (mapOfOrderNo.size() == 0)) {
			return;
		}

		sBufOrderNo = new StringBuilder((mapOfOrderNo.size()+1)*13);
		iter = mapOfOrderNo.keySet().iterator();
		sBufOrderNo.append('(');
		while (iter.hasNext()) {
			sBufOrderNo.append('\'');
			sBufOrderNo.append(iter.next());
			sBufOrderNo.append("',");
		}
		sBufOrderNo.setCharAt(sBufOrderNo.length()-1, ')');

		
		
		/*order item */
		sQuery = "SELECT ITEM.SALES_DOC_ORDER_NO, "+ 
		"ITEM.SALES_DOC_ITEM_NO, " +
		"RTRIM(ITEM.PROMO_ID_1 ||' '|| ITEM.PROMO_ID_2 ||' '|| ITEM.PROMO_ID_3 ||' '|| ITEM.PROMO_ID_4 ||' '|| ITEM.PROMO_ID_5) AS PROMO_ID, " +
		"RTRIM(ITEM.MACH_TYPE_MOD) AS TYPEMODEL, " +
		"RTRIM(ITEM.PROFIT_CENTER) AS PROFIT_CENTER, " +
		"CASE WHEN (ITEM.BLB_INDC<>'') THEN 'Y' ELSE 'N' END AS BLB_INDC, " +
		"RTRIM(ITEM.RPO_MES_INDC) AS RPO_MES_INDC, "+ 
		"RTRIM(ITEM.MODEL) AS MACH_MOD, "+ 
		"RTRIM(ITEM.MES_MACH_TYPE) AS MACH_TYPE, " +
		"RTRIM(ITEM.SPEC_BID_INDC) AS SPEC_BID_INDC, "+
		"RTRIM(ITEM.INST_TYPE) AS INST_TYPE, " +
		"RTRIM(ITEM.PRECED_DOC_NO) AS PRECED_DOC_NO, "+ 
		"RTRIM(ITEM.PRECED_DOC_ITEM_NO) AS PRECED_DOC_ITEM_NO, " +
		"RTRIM(ITEM.PURCHASE_ORD_NO) AS PURCHASE_ORD_NO, "+ 
		"RTRIM(ITEM.ORIG_LINE_ITEM) AS ORIG_LINE_ITEM," +
		"RTRIM(ITEM.HGH_LEV_ITEM) AS HGH_LEV_ITEM, " +
		"RTRIM(ITEM.PROD_TYPE_INDC), " +
		"ITEM.NET_ITEM_VAL_DOC AS ITEM_AMOUNT, " +
		"CASE WHEN (ITEM.REQ_DELIV_QTY=0) THEN ITEM.NET_ITEM_VAL_DOC WHEN (ITEM.REQ_DELIV_QTY=1) THEN ITEM.NET_ITEM_VAL_DOC ELSE DEC((FLOAT(ITEM.NET_ITEM_VAL_DOC) / FLOAT(ABS(ITEM.REQ_DELIV_QTY))),17,4) END AS UNIT_AMOUNT, " +
		//"ITEM.NET_UNIT_PRC_DOC AS UNIT_AMOUNT, " +
		"ITEM.REQ_DELIV_QTY AS ITEM_QTY, " +
		"RTRIM(ITEM.LEGAL_CONTRCT_CPS_NO) AS LEGAL_CONTRCT_CPS_NO, " + 
		"RTRIM(ITEM.MATERIAL_NO) AS MATERIAL_NO, "+
		"'' AS USAGE_TYPE, " +
		"'' AS QUOTE_TYPE, " + 
		"RTRIM(ITEM.ITEM_CATG) AS  ITEM_CATG, "+ 
		"'' AS PRODUCT_DIV_BM, "+
		"RTRIM(SPEC_BID_INDC), " +
		"'' AS SPEC_BID_INDC_QH, " +
		"'' AS SPEC_BID_INDC_QI,  " +
		"'' AS QUOTE_DATE,  " +
		"'' AS QUOTE_DESC,  " +
		"'' AS OPPT_ID  " +
		"FROM WSDIW.CHSW_WW_ORD_ITEM ITEM "
		+ "WHERE (ITEM.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ") " +
		//"AND (ITEM.NET_ITEM_VAL_DOC <> 0 )" + impact performance if added this criteria
		"ORDER BY ITEM.SALES_DOC_ORDER_NO, ITEM.SALES_DOC_ITEM_NO " +
		"FOR READ ONLY ";

		
		Map<String, String> mapOfQuoteItem = new HashMap<String, String>();
		Map<String, String[]> mapOfItem = new HashMap<String, String[]>();
		Map<String, String> mapOfFeatureItem = new HashMap<String, String>();

		rs = getDbStatement(false).executeQuery(sQuery);
		rsmd = rs.getMetaData();
	    iColCount = rsmd.getColumnCount();
	    iCount = 0;
		while (rs.next()) {
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    }
		    if(ar_sField[11].equals(ar_sField[0])) { 
 		    	try {
		    		ar_sField[11]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[0])))[6];
		    	} catch (Exception e) {}
		    }

		    mapOfItem.put(ar_sField[0] + "|" + ar_sField[1], ar_sField);
		    if ((ar_sField[11]!=null) && (ar_sField[11] != ar_sField[0])){
			    if (mapOfQuoteItem.containsKey(ar_sField[11] + "|" + ar_sField[12])) {
			    	mapOfQuoteItem.put(ar_sField[11] + "|" + ar_sField[12], mapOfQuoteItem.get(ar_sField[11] + "|" + ar_sField[12]) + "\n" + ar_sField[0] + "|" + ar_sField[1]);
			    } else {
			    	mapOfQuoteItem.put(ar_sField[11] + "|" + ar_sField[12], ar_sField[0] + "|" + ar_sField[1]);
			    }
		    }

		    //get feature installation type
		    if ((ar_sField[21].startsWith(ar_sField[21].substring(0, 4)+"F")) && (ar_sField[21].trim().length()==9)) {
		    	mapOfFeatureItem.put(ar_sField[0] + "|" + ar_sField[15] + "|" + ar_sField[21] , ar_sField[10]);
		    }
		    
			iCount++;
		}
		
		rs.close();

		/*order item */
		sQuery = "SELECT ITEM_DETAIL.SALES_DOC_ORDER_NO, "+ 
		"ITEM_DETAIL.SALES_DOC_ITEM_NO, " +
		"ITEM_DETAIL.PRODUCT_DIV_BM  "+
		"FROM WSDIW.CHSW_WW_ORD_DETAIL ITEM_DETAIL "
		+ "WHERE (ITEM_DETAIL.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ") " +
		"FOR READ ONLY ";

		Map<String, String> mapOfItemDetail = new HashMap<String, String>();
		rs = getDbStatement(false).executeQuery(sQuery);
		rsmd = rs.getMetaData();
	    iColCount = rsmd.getColumnCount();
	    iCount = 0;
		while (rs.next()) {
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    }

		    mapOfItemDetail.put(ar_sField[0] + "|" + ar_sField[1], ar_sField[2]);
		    
			iCount++;
		}
		
		rs.close();
		
		//service contract
		Map<String, String[]> mapOfContractItem = new HashMap<String, String[]>();
		Map<String, String[]> mapOfIbaseContLink = new HashMap<String, String[]>();
		Map<String, String> mapOfContItemIbase = new HashMap<String, String>();
		Map<String, String> mapOfSNPlant = new HashMap<String, String>();
		
		//CPS to quote
		if (mapOfCPSQuoteNo.size() > 0) {
			StringBuilder sBufCPSQuoteNo = new StringBuilder((mapOfCPSQuoteNo.size()+1)*13);
			iter = mapOfCPSQuoteNo.keySet().iterator();
			sBufCPSQuoteNo.append('(');
			while (iter.hasNext()) {
				sBufCPSQuoteNo.append('\'');
				sBufCPSQuoteNo.append(iter.next());
				sBufCPSQuoteNo.append("',");
			}
			sBufCPSQuoteNo.setCharAt(sBufCPSQuoteNo.length()-1, ')');
			mapOfCPSQuoteNo.clear();
			sQuery = "SELECT RTRIM(QUOTE_DOC_NO) AS QUOTE_DOC_NO, "
					+ "RTRIM(CPS_SPECIAL_BID_NO) AS LEGAL_CPS_CONTRACT " +
			"FROM WSDIW.CHSW_WW_QUOTE_HEADER WHERE (CPS_SPECIAL_BID_NO IN " + sBufCPSQuoteNo.toString() + ") " +
			"FOR READ ONLY ";
			rs = getDbStatement(false).executeQuery(sQuery);
			rsmd = rs.getMetaData();
		    iColCount = rsmd.getColumnCount();
		    iCount = 0;
			while (rs.next()) {
				ar_sField = new String[iColCount];
			    for (int r=1; r <= iColCount; r++) {
			    	ar_sField[r-1] = rs.getString(r);
			    }
			    mapOfCPSQuoteNo.put(ar_sField[0], ar_sField[1]);
				iCount++;
			}
			rs.close();		
			sBufCPSQuoteNo = null;
			
			if (mapOfCPSQuoteNo.size() > 0) {
				StringBuilder sBufCPSQuoteItem = new StringBuilder((mapOfCPSQuoteNo.size()+1)*13);
				iter = mapOfCPSQuoteNo.keySet().iterator();
				sBufCPSQuoteItem.append('(');
				while (iter.hasNext()) {
					sBufCPSQuoteItem.append('\'');
					sBufCPSQuoteItem.append(iter.next());
					sBufCPSQuoteItem.append("',");
				}
				sBufCPSQuoteItem.setCharAt(sBufCPSQuoteItem.length()-1, ')');

				sQuery = "SELECT RTRIM(QUOTE_DOC_NO) AS QUOTE_DOC_NO, "
						+ "QUOTE_DOC_ITEM_NO,"
						+ "RTRIM(QUOTE_ITEM_REF) AS QUOTE_ITEM_REF " +
				"FROM WSDIW.CHSW_WW_QUOTE_ITEM WHERE (QUOTE_DOC_NO IN " + sBufCPSQuoteItem.toString() + ") " +
				"FOR READ ONLY ";
				rs = getDbStatement(false).executeQuery(sQuery);
				rsmd = rs.getMetaData();
			    iColCount = rsmd.getColumnCount();
			    iCount = 0;
				while (rs.next()) {
					ar_sField = new String[iColCount];
				    for (int r=1; r <= iColCount; r++) {
				    	ar_sField[r-1] = rs.getString(r);
				    }
				    mapOfCPSQuoteItem.put(ar_sField[2], ar_sField);
					iCount++;
				}
				rs.close();		
				sBufCPSQuoteItem = null;
			}
		}
		
		if (mapOfSrvContractNo.size() > 0) {	
			
			StringBuilder sBufSrvContractNo = new StringBuilder((mapOfSrvContractNo.size()+1)*13);
			iter = mapOfSrvContractNo.keySet().iterator();
			sBufSrvContractNo.append('(');
			while (iter.hasNext()) {
				sBufSrvContractNo.append('\'');
				sBufSrvContractNo.append(iter.next());
				sBufSrvContractNo.append("',");
			}
			sBufSrvContractNo.setCharAt(sBufSrvContractNo.length()-1, ')');
			mapOfSrvContractNo.clear();
			
			sQuery = "SELECT RTRIM(CONTI.DOCUMENT_NO) AS DOCUMENT_NO, "
					+ "RTRIM(CONTI.NUMBER_INT) AS NUMBER_INT, "
					+ "RTRIM(CONTI.QUOTE_DOC_NO) AS QUOTE_DOC_NO, "
					+ "RTRIM(CONTI.QUOTE_DOC_ITEM_NO) AS QUOTE_DOC_ITEM_NO, "
					+ "RTRIM(CONTI.CPS_ID_ORIG) AS LEGAL_CONTRACT_CPS_NO,"
					+ "RTRIM(CONTI.IBASE_REF_LINK) AS IBASE_REF_LINK,"
					+ "'' AS SALES_ORDER_DOC_NO,"
					+ "'' AS SALES_ORDER_ITEM,"
					+ "'' AS SERIAL_NO " +
			"FROM WSDIW.CHSW_WW_CONT_I CONTI WHERE (CONTI.DOCUMENT_NO IN " + sBufSrvContractNo.toString() + ") " +
			"FOR READ ONLY ";

			Map<String, String> mapOfIbaseContract = new TreeMap<String, String>();
		
			rs = getDbStatement(false).executeQuery(sQuery);
			rsmd = rs.getMetaData();
		    iColCount = rsmd.getColumnCount();
		    iCount = 0;
			while (rs.next()) {
				ar_sField = new String[iColCount];
			    for (int r=1; r <= iColCount; r++) {
			    	ar_sField[r-1] = rs.getString(r);
			    }
			    
	  
			    if ((ar_sField[2] != null) && (ar_sField[2].trim().length() > 0)) {
			    	
			    	//replace ref_quote
			    	if (mapOfCPSQuoteItem.containsKey(ar_sField[2] +"-"+ar_sField[3])){
			    		String Ref_quoteitem;
			    		Ref_quoteitem = ar_sField[2] +"-"+ar_sField[3];
			    		ar_sField[2] = mapOfCPSQuoteItem.get(Ref_quoteitem)[0];
			    		ar_sField[3] = mapOfCPSQuoteItem.get(Ref_quoteitem)[1];
			    	}
			    	
				    if (mapOfQuoteItem.containsKey(ar_sField[2] + "|" + ar_sField[3])) {
				    	mapOfQuoteItem.put(ar_sField[2] + "|" + ar_sField[3], mapOfQuoteItem.get(ar_sField[2] + "|" + ar_sField[3]) + "\n" + ar_sField[0] + "|" + ar_sField[1]);
				    } else {
				    	mapOfQuoteItem.put(ar_sField[2] + "|" + ar_sField[3], ar_sField[0] + "|" + ar_sField[1]);
				    }
				    mapOfQuote.put(ar_sField[2], ar_sField[4]);
			    }
			    if (mapOfItem.containsKey(ar_sField[0] + "|" + ar_sField[1])){
			    	mapOfItem.get(ar_sField[0] + "|" + ar_sField[1])[11] = ar_sField[2];
			    	mapOfItem.get(ar_sField[0] + "|" + ar_sField[1])[12] = ar_sField[3];
			    }
				
			    mapOfContractItem.put(ar_sField[0] + "|" + ar_sField[1], ar_sField);
				
				if ((ar_sField[5] != null) && (ar_sField[5].trim().length() > 0)) {
				    if (mapOfContItemIbase.containsKey(ar_sField[5])) {
				    	mapOfContItemIbase.put(ar_sField[5], mapOfContItemIbase.get(ar_sField[5]) + "\n" + ar_sField[0] + "|" + ar_sField[1]);
				    } else {
				    	mapOfContItemIbase.put(ar_sField[5], ar_sField[0] + "|" + ar_sField[1]);
				    }
					mapOfIbaseContract.put(ar_sField[5], null);
				}
				iCount++;
			}
			
			rs.close();
			sBufSrvContractNo = null;

			if (mapOfIbaseContract.size() > 0) {
			
				StringBuilder sBufIbaseContract = new StringBuilder((mapOfIbaseContract.size()+1)*13);
				sBufIbaseContract.setLength(0);
				iter = mapOfIbaseContract.keySet().iterator();
				sBufIbaseContract.append('(');
				while (iter.hasNext()) {
					sBufIbaseContract.append('\'');
					sBufIbaseContract.append(iter.next());
					sBufIbaseContract.append("',");
				}
				sBufIbaseContract.setCharAt(sBufIbaseContract.length()-1, ')');
				mapOfIbaseContract.clear();
				sQuery = "SELECT RTRIM(IBASE.SALESORDER) AS SALESORDER,"
						+ "RTRIM(IBASE.SALESORDER_ITEM) AS SALESORDER_ITEM, "
						+ "RTRIM(IBASE.SERIAL_NUMBER) AS SERIAL_NUMBER,"
						+ "IBASE.CONTRACT_REF_LINK " + 
				"FROM WSDIW.CHSW_WW_IBASE_INVENTORY IBASE "
				+ "WHERE (IBASE.CONTRACT_REF_LINK IN " + sBufIbaseContract.toString() + ") " +
				"FOR READ ONLY ";
				
				Map<String, String> mapOfIbaseOrder = new TreeMap<String, String>();
				rs = getDbStatement(false).executeQuery(sQuery);
				rsmd = rs.getMetaData();
			    iColCount = rsmd.getColumnCount();
			    iCount = 0;
			    String sKey = null;
			    StringTokenizer sTok = null;
			    String sKeyTok = null;

				while (rs.next()) {
					ar_sField = new String[iColCount];
				    for (int r=1; r <= iColCount; r++) {
				    	ar_sField[r-1] = rs.getString(r);
				    }
				    
				    mapOfIbaseOrder.put(ar_sField[0], null);
				    
					sKey = rs.getString(4);
				    if ((sKey == null) || (sKey.length() == 0)) {
				    	continue;
				    }
				    sKey = mapOfContItemIbase.get(rs.getString(4) );
				    if (sKey == null) {
				    	continue;
				    }
				    sTok = new StringTokenizer(sKey, "\n");
				    while (sTok.hasMoreTokens()) {
				    	sKeyTok = sTok.nextToken();
					    if (mapOfContractItem.containsKey(sKeyTok)) {
					    	mapOfContractItem.get(sKeyTok)[6]=rs.getString(1);
					    	mapOfContractItem.get(sKeyTok)[7]=rs.getString(2);
					    	mapOfContractItem.get(sKeyTok)[8]=rs.getString(3);
					    }
				    }
					iCount++;
				}
				
				rs.close();
				sBufIbaseContract = null;
				
				if (mapOfIbaseOrder.size() > 0) {
					StringBuilder sBufIbaseOrder = new StringBuilder((mapOfIbaseOrder.size()+1)*13);
					sBufIbaseOrder.setLength(0);
					iter = mapOfIbaseOrder.keySet().iterator();
					sBufIbaseOrder.append('(');
					while (iter.hasNext()) {
						sBufIbaseOrder.append('\'');
						sBufIbaseOrder.append(iter.next());
						sBufIbaseOrder.append("',");
					}
					sBufIbaseOrder.setCharAt(sBufIbaseOrder.length()-1, ')');
					mapOfIbaseOrder.clear();
					sQuery = "SELECT SERL.SALES_DOC_ORDER_NO, SERL.SALES_DOC_ITEM_NO, " +
							"RTRIM(SERL.SYSTEM_NO) AS SYSTEM_NO, RTRIM(SERL.EQIP_SERIAL_NO) AS EQIP_SERIAL_NO, RTRIM(SERL.PLANT_ORDER_NO) AS PLANT_ORDER_NO, " +
							"SERL.GOODS_ISSUE_DATE AS PLANT_SHIP_DATE " +
							"FROM WSDIW.CHSW_WW_ORD_EQUI SERL WHERE (SERL.SALES_DOC_ORDER_NO IN " + sBufIbaseOrder.toString() + ") " +
							"AND SERL.EQIP_SERIAL_NO !='' " +
							"FOR READ ONLY ";
					
					rs = getDbStatement(false).executeQuery(sQuery);
					rsmd = rs.getMetaData();
				    iColCount = rsmd.getColumnCount();
				    iCount = 0;
					while (rs.next()) {
						ar_sField = new String[iColCount];
					    for (int r=1; r <= iColCount; r++) {
					    	ar_sField[r-1] = rs.getString(r);
					    }
					    mapOfSNPlant.put(ar_sField[0] + "|" + ar_sField[1] + "|" + ar_sField[3], ar_sField[4]);
						iCount++;
					}
					
					rs.close();		
					sBufIbaseOrder = null;
				}

			}
			
		}		
		
		/*order partner address */
		sQuery = "SELECT SADR.SALES_DOC_ORDER_NO, "+ 
		"SADR.PRTNR_NO, " +
		"SADR.CUST_NAME " +
		"FROM WSDIW.CHSW_WW_ORD_SADR SADR WHERE (SADR.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ") " +
		"AND SADR.PRTNR_FUNC_ID = 'TC' " +
		"FOR READ ONLY ";

		Map<String, String[]> mapOfSADR = new HashMap<String, String[]>();

		rs = getDbStatement(false).executeQuery(sQuery);
		rsmd = rs.getMetaData();
	    iColCount = rsmd.getColumnCount();
	    iCount = 0;
		while (rs.next()) {
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    }
		    
		    mapOfSADR.put(ar_sField[0], ar_sField);		    
			iCount++;
		}
		
		rs.close();
		
		
		sQuery = "SELECT BILL.CUSTNO_SOLD_TO, " + 
				"BILL.SALES_DOC_ORDER_NO AS SALES_DOC_ORDER_NO, "+ 
				"BILL.SALES_DOC_ITEM_NO AS SALES_DOC_ITEM_NO, " +
				"RTRIM(BILL.LEGAL_CONTRCT_CPS_NO) AS LEGAL_CONTRCT_CPS_NO, " + 
				"RTRIM(BILL.MATERIAL_NO) AS MATERIAL_NO, "+
				"RTRIM(BILL.ITEM_DESCRIPTION) AS ITEM_DESCRIPTION, "+
				"RTRIM(BILL.CLS_TYPE) AS CLASS_TYPE, " + 
				"BILL.BILL_DATE, "+
				"BILL.BILL_DOC_NO, "+
				"BILL.BILL_DOC_ITEM_NO, " +
				"BILL.NET_ITEM_VAL_DOC AS BILL_AMOUNT, " +
				"RTRIM(BILL.BILL_DOC_CRCY) AS BILL_DOC_CRCY, "+
				"BILL.BILL_ITEM_QTY, " +
				"CASE WHEN (BILL.BILL_ITEM_QTY=0) THEN BILL.NET_ITEM_VAL_DOC WHEN (BILL.BILL_ITEM_QTY=1) THEN BILL.NET_ITEM_VAL_DOC ELSE DEC((FLOAT(BILL.NET_ITEM_VAL_DOC) / FLOAT(ABS(BILL.BILL_ITEM_QTY))),17,4) END AS BILLUNIT_AMOUNT, " +
				//rounding issues//
				//"BILL.NET_UNIT_VAL_DOC AS BILLUNIT_AMOUNT, " +
				"RTRIM(BILL.BILL_TYPE_CD) AS BILL_TYPE, "+
				"CASE WHEN (BILL.BILL_TYPE_CD IN ('ZF2','ZS1')) THEN 'Y' ELSE 'N' END AS BILL_EXT, " +
				"BILL.SALES_ORG, RTRIM(BILL.SALES_GRP) AS SALES_GROUP, "+
				"RTRIM(BILL.CUSTNO_LGCY_SOLD_TO) AS CUSTNO_LGCY_SOLD_TO, " +
				"IW_VALID_FROM_TS AS RECORD_TS, " + 
				"CUSTNO_BILL_TO AS BILLTO_NO, " +
				"RTRIM(BILL.CANCELLED_BILL) AS CANCELLED_BILL, "+
				"RTRIM(BILL.CNCL_BILL_DOC_NO) AS CANCELLED_BILL_NO, " +
				"RTRIM(BILL.ORDER_REASON) AS ORDER_REASON, " +
				"BILL.BILL_ITEM_QTY_NEG, "+
				"BILL.BILL_TYPE_CD, "+
				"BILL.IW_VALID_FROM_TS  "+
				"FROM WSDIW.CHSW_WW_BILL BILL " +
				"WHERE " +
				"( ( (BILL.BILL_TYPE_CD IN ('ZF2','ZS1','ZF2I','ZS1I')) " + 
				" AND (BILL.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ")) ) " +
				//((mapOfBLBCPSNo.size() > 0) ? "OR (BILL.LEGAL_CONTRCT_CPS_NO IN " + sBufBLBCPSNo.toString() + " )) " : " ) ") +
				//"AND ((BILL.CANCELLED_BILL IS NULL) OR (BILL.CANCELLED_BILL NOT IN ('+','-'))) " + 
				"AND (BILL.IW_VALID_FROM_TS > TIMESTAMP('" + m_sIwTs + "') " +
		        "AND BILL.IW_VALID_FROM_TS < TIMESTAMP('" + m_sIwTs_end + "')) " +
				"AND (BILL.BILL_ITEM_QTY<>0) " + 
				"AND (BILL.NET_ITEM_VAL_DOC <> 0) " +
				"AND (BILL.CLS_TYPE != '' ) " +
				"ORDER BY 4, 2 DESC, 3, 9, 10 " +
				"FOR READ ONLY ";
				
		Map<String, String> mapOfFeaturesMIC = new HashMap<String, String>();
		List<String[]> listOfInvoice = new ArrayList<String[]>();
		Map<String, Integer> mapOfInvoice = new HashMap<String, Integer>();
		rs = getDbStatement(false).executeQuery(sQuery);
		rsmd = rs.getMetaData();
	    iColCount = rsmd.getColumnCount();
	    
	    iCount = 0;
		while (rs.next()) {
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    }
/*		    if ((ar_sField[4].startsWith("69SA"))&&(ar_sField[1].startsWith("74"))) {
		    	if (ar_sField[4].equals("69SAX02")) {
			        mapOfInvoice.put(ar_sField[1] + "|" + ar_sField[2], Integer.valueOf(iCount));
			        listOfInvoice.add(ar_sField);
		    	}
		    }else{
		    	mapOfInvoice.put(ar_sField[1] + "|" + ar_sField[2], Integer.valueOf(iCount));
		        listOfInvoice.add(ar_sField);
		    }*/
	    	mapOfInvoice.put(ar_sField[1] + "|" + ar_sField[2], Integer.valueOf(iCount));
	        listOfInvoice.add(ar_sField);
			iCount++;
		}
		rs.close();
		if (iCount == 0) {
			return;
		}	
		
		
		
		sQuery = "SELECT FEAT.SALES_DOC_ORDER_NO " +
		"FROM WSDIW.CHSW_WW_ORD_FEATURES FEAT INNER JOIN WSDIW.CHSW_WW_ORD_HEADER HDR ON FEAT.SALES_DOC_ORDER_NO = HDR.SALES_DOC_ORDER_NO " + 
		"WHERE (FEAT.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ") " +
		"AND HDR.SALES_DOC_TYPE = 'ZGOR' " +
		"AND (FEAT.FEAT_CD='ECS0') " +
		"AND (FEAT.ADD_ON_INDC='Y') " +
		"FOR READ ONLY ";
		
		rs = getDbStatement(false).executeQuery(sQuery);
	    iCount = 0;
		while (rs.next()) {
		    mapOfFeaturesMIC.put(rs.getString(1), "X");
			iCount++;
		}
		
		rs.close();
		
		sQuery = "SELECT SERL.SALES_DOC_ORDER_NO, SERL.SALES_DOC_ITEM_NO, " +
		"RTRIM(SERL.SYSTEM_NO) AS SYSTEM_NO, RTRIM(SERL.EQIP_SERIAL_NO) AS EQIP_SERIAL_NO, RTRIM(SERL.PLANT_ORDER_NO) AS PLANT_ORDER_NO, " +
		"SERL.GOODS_ISSUE_DATE AS PLANT_SHIP_DATE " +
		"FROM WSDIW.CHSW_WW_ORD_EQUI SERL WHERE (SERL.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ") " +
		"AND SERL.EQIP_SERIAL_NO !='' " +
		"ORDER BY 1, 2, 6 DESC, 5 " +
		"FOR READ ONLY ";
		
		List<String[]> listOfEqui = new ArrayList<String[]>();
		Map<String, Integer> mapOfEqui = new HashMap<String, Integer>();
		
		rs = getDbStatement(false).executeQuery(sQuery);
		rsmd = rs.getMetaData();
	    iColCount = rsmd.getColumnCount();
	    int iEquiArraySize = iColCount;		

	    iCount = 0;
		while (rs.next()) {
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    }

		    if (!mapOfEqui.containsKey(ar_sField[0] + "|" + ar_sField[1])) {
		    	mapOfEqui.put(ar_sField[0] + "|" + ar_sField[1], Integer.valueOf(iCount));
		    }
		    listOfEqui.add(ar_sField);
		    //mapOfSNPlant.put(ar_sField[0] + "|" + ar_sField[1] + "|" + ar_sField[3], ar_sField[4]);
			iCount++;
		}
		
		rs.close();
	    
		/*Features details begin*/
		sQuery = "SELECT FEAT.SALES_DOC_ORDER_NO, FEAT.SALES_DOC_ITEM_NO, " +
		"FEAT.MACHINE_TYPE, FEAT.MODEL, FEAT.FEAT_CD, FEAT.FEAT_QTY " +
        "FROM WSDIW.CHSW_WW_ORD_FEATURES FEAT INNER JOIN WSDIW.CHSW_WW_ORD_ITEM ITEM ON FEAT.SALES_DOC_ORDER_NO = ITEM.SALES_DOC_ORDER_NO AND FEAT.SALES_DOC_ITEM_NO = ITEM.SALES_DOC_ITEM_NO " + 
		"WHERE FEAT.ADD_ON_INDC = 'Y' AND FEAT.FEAT_CD != '' AND ITEM.PROD_TYPE_INDC = 'BLK' " +
		"AND (FEAT.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ") " +
		"ORDER BY FEAT.SALES_DOC_ORDER_NO, FEAT.SALES_DOC_ITEM_NO " +
		"FOR READ ONLY ";

		Map<String, Integer> mapOfFeaturesItem = new HashMap<String, Integer>();
		List<String[]> listOfFeatures = new ArrayList<String[]>();
		rs = getDbStatement(false).executeQuery(sQuery);
		rsmd = rs.getMetaData();
	    iColCount = rsmd.getColumnCount();
	    iCount = 0;
		while (rs.next()) {
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    }
			if (!mapOfFeaturesItem.containsKey(ar_sField[0] + "|" + ar_sField[1])) {
		    	mapOfFeaturesItem.put(ar_sField[0] + "|" + ar_sField[1], Integer.valueOf(iCount));
		    }
            listOfFeatures.add(ar_sField);
			iCount++;
		}
		
		rs.close();
        /*Features details end*/	
		
		/*Features machine type model begin*/
		sQuery = "SELECT FEAT.SALES_DOC_ORDER_NO, FEAT.SALES_DOC_ITEM_NO, " +
		"FEAT.MACHINE_TYPE, FEAT.MODEL " +
        "FROM WSDIW.CHSW_WW_ORD_FEATURES FEAT " + 
		"WHERE FEAT.FEAT_CD = ''  " +
		"AND (FEAT.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ") " +
		"FOR READ ONLY ";

		Map<String, String[]> mapOfFeaturesModel = new HashMap<String, String[]>();
		rs = getDbStatement(false).executeQuery(sQuery);
		rsmd = rs.getMetaData();
	    iColCount = rsmd.getColumnCount();
	    iCount = 0;
		while (rs.next()) {
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    }
		    mapOfFeaturesModel.put(ar_sField[0] + "|" + ar_sField[1], ar_sField );
			iCount++;
		}
		
		rs.close();
        /*Features machine type model end*/			
		


		//Party address
		List<String[]> listOfAddress = new ArrayList<String[]>();
		Map<String, Integer> mapOfAddress = new HashMap<String, Integer>();
		Map<String, String[]> mapOfEndUserId = new TreeMap<String, String[]>();
		Map<String, String[]> mapOfAddressCRM = new TreeMap<String, String[]>();
		Map<String, String[]> mapOfT3CRM = new TreeMap<String, String[]>();
		Map<String, String[]> mapOfBPId = new TreeMap<String, String[]>();
		if (mapOfQuote.size() > 0) {
			StringBuilder sBufQuote = new StringBuilder((mapOfQuote.size()+1)*13);
			iter = mapOfQuote.keySet().iterator();
			sBufQuote.append('(');
			while (iter.hasNext()) {
				sBufQuote.append('\'');
				sBufQuote.append(iter.next());
				sBufQuote.append("',");
			}
			sBufQuote.setCharAt(sBufQuote.length()-1, ')');
			
			sQuery = "SELECT CRM.QUOTE_DOC_NO, CRM.QUOTE_DOC_ITEM_NO, " +
			"RTRIM(CRM.PRTNR_FUNC) AS ADDRESS_TYPE, RTRIM(CRM.PRTNR_NO) AS SITEID, RTRIM(CRM.CUST_NAME) || RTRIM(CRM.CUST_NAME_2) AS NAME, " +
			"RTRIM(CRM.CITY) AS CITY, RTRIM(CRM.CUST_NAME) AS CUST_NAME_NATION, '' AS CUSTNO_LGCY, " +
			"RTRIM(CRM.FULL_NAME) AS CONTACT_NAME, RTRIM(STREET) || RTRIM(CITY) || RTRIM(POSTAL_CODE) AS ADDRESS " +
			//"FROM WSDIW.CHSW_WW_QUOTE_PARTNER CRM WHERE (CRM.PRTNR_FUNC IN ('00000003','ZTC','ZEC','ZCO','ZCF')) AND (CRM.QUOTE_DOC_ITEM_NO=0) " +
			"FROM WSDIW.CHSW_WW_QUOTE_PARTNER CRM WHERE (CRM.PRTNR_FUNC IN ('00000001','00000003','ZTC','ZEC','ZCO','ZCT','ZMC')) " +
			"AND (CRM.QUOTE_DOC_NO IN " + sBufQuote.toString() + ") " +
			"ORDER BY 1, 2 " +
			"FOR READ ONLY ";
			
			rs = getDbStatement(false).executeQuery(sQuery);
			rsmd = rs.getMetaData();
		    iColCount = rsmd.getColumnCount();
		    
		    iCount = 0;
			while (rs.next()) {
				ar_sField = new String[iColCount];
			    for (int r=1; r <= iColCount; r++) {
			    	ar_sField[r-1] = rs.getString(r);
			    }
			    //ar_sField[0] = mapOfQuote.get(ar_sField[0]);
			    //ar_sField[1] = "0";
			    if  ((ar_sField[2].equals("ZEC")) || (ar_sField[2].equals("ZMC"))) {
				    if (!ar_sField[3].startsWith("BP")) {
				    	mapOfAddressCRM.put(ar_sField[0] + "|" + ar_sField[1], ar_sField);
				    	mapOfEndUserId.put(ar_sField[3], null);
				    }
/*			    } else if (ar_sField[2].equals("ZCF")) {
			    	mapOfT3CRM.put(ar_sField[0] + "|" + ar_sField[1], ar_sField);*/
			    } else {
			    	if (ar_sField[1].equals("0")){
				    	if (!mapOfAddress.containsKey(ar_sField[0] + "|" + ar_sField[1])) {
					    	mapOfAddress.put(ar_sField[0] + "|" + ar_sField[1], Integer.valueOf(iCount));
					    }
				    	mapOfBPId.put(ar_sField[3], null);
			    	}

			    }
			    listOfAddress.add(ar_sField);
				iCount++;
			}
			
			rs.close();
			sBufQuote = null;
			}

		//Quote text
		Map<String, String> mapOfQuoteText = new TreeMap<String, String>();
		if (mapOfQuote.size() > 0) {
			StringBuilder sBufQuote = new StringBuilder((mapOfQuote.size()+1)*13);
			iter = mapOfQuote.keySet().iterator();
			sBufQuote.append('(');
			while (iter.hasNext()) {
				sBufQuote.append('\'');
				sBufQuote.append(iter.next());
				sBufQuote.append("',");
			}
			sBufQuote.setCharAt(sBufQuote.length()-1, ')');
			
			sQuery = "SELECT QUOTE_DOC_NO, RTRIM(TXT_LINE_QUOTE_ITEM) AS TA_INDC " 
			+ " FROM WSDIW.CHSW_WW_QUOTE_TEXT  "
			+ " WHERE QUOTE_DOC_NO IN " + sBufQuote.toString() 
			+ " AND QUOTE_DOC_ITEM_NO = '0' "
			+ " AND TXT_ID_QUOTE_ITEM = 'ZAGT'"
			+ " FOR READ ONLY ";
			
			rs = getDbStatement(false).executeQuery(sQuery);
			rsmd = rs.getMetaData();
		    iColCount = rsmd.getColumnCount();
		    
		    iCount = 0;
			while (rs.next()) {
				ar_sField = new String[iColCount];
			    for (int r=1; r <= iColCount; r++) {
			    	ar_sField[r-1] = rs.getString(r);
			    }

			    if  (ar_sField[1].startsWith("TA") ) {

			    	mapOfQuoteText.put(ar_sField[0], "TA");
			    }

				iCount++;
			}
			
			rs.close();
			sBufQuote = null;
		}//Quote text
		
		
		Map<String, String> mapOfMaterial = new TreeMap<String, String>();
		sQuery = "SELECT RTRIM(PROFIT_CENTER) AS PROFIT_CENTER, RTRIM(LONG_TEXT) AS BRAND_DESC " +
		"FROM WSDIW.IM_PROFIT_CENTER_REF REF " +
		"FOR READ ONLY ";

		rs = getDbStatement(false).executeQuery(sQuery);
	    iCount = 0;
		while (rs.next()) {
			mapOfMaterial.put(rs.getString(1), rs.getString(2));
			iCount++;
		}
		
		rs.close();
		
		sQuery = "SELECT FEAT.SALES_DOC_ORDER_NO, FEAT.SALES_DOC_ITEM_NO " +
		"FROM WSDIW.CHSW_WW_ORD_FEATURES FEAT " + 
		"WHERE (FEAT.SALES_DOC_ORDER_NO IN " + sBufOrderNo.toString() + ") " +
		"AND (FEAT.FEAT_CD='0002' or FEAT.FEAT_CD='0003') " +
		"FOR READ ONLY ";
		
		Map<String, String> mapOfFeaturesSDI = new HashMap<String, String>();
		rs = getDbStatement(false).executeQuery(sQuery);
		rsmd = rs.getMetaData();
	    iColCount = rsmd.getColumnCount();
	    
	    iCount = 0;
		while (rs.next()) {
			ar_sField = new String[iColCount];
		    for (int r=1; r <= iColCount; r++) {
		    	ar_sField[r-1] = rs.getString(r);
		    }
		    mapOfFeaturesSDI.put(ar_sField[0] + "|" + ar_sField[1], "X");
		    mapOfFeaturesSDI.put(ar_sField[0], "X");
			iCount++;
		}
		
		rs.close();

		if (mapOfEndUserId.size() > 0) {
			StringBuilder sBufSiteId = new StringBuilder((mapOfEndUserId.size()+1)*13);
			iter = mapOfEndUserId.keySet().iterator();
			sBufSiteId.append('(');
			while (iter.hasNext()) {
				sBufSiteId.append('\'');
				sBufSiteId.append(iter.next());
				sBufSiteId.append("',");
			}
			sBufSiteId.setCharAt(sBufSiteId.length()-1, ')');
//			sQuery = "SELECT CISF.SITE_PARTY_ID, CISF.ISU, CISF.CURR_COVR_ID, CISF.LEGAL_NAME_SITE FROM WSDIW.IM_WW_CCM_REF_CISF CISF " +
//			"WHERE (CISF.SITE_PARTY_ID IN " + sBufSiteId.toString() + ") " +
//			"FOR READ ONLY ";
			sQuery = "SELECT MDM.SITE_PARTY_ID, "
					+ "CISF.ISU, "
					+ "CISF.CURR_COVR_ID, "
					+ "RTRIM(MDM.NAME1) || ' ' || RTRIM(MDM.NAME2), "
					+ "MDM.CUSTNO " 
					+ "FROM WSDIW.MDM_WW_CUSTOMER MDM INNER JOIN "
					+ "WSDIW.IM_WW_CCM_REF_CISF CISF "
					+ "ON MDM.KUNNR_MPP = CISF.KUNNR " +
			"WHERE (MDM.SITE_PARTY_ID IN " + sBufSiteId.toString() + ") " +
			"FOR READ ONLY ";
			
			rs = getDbStatement(false).executeQuery(sQuery);
			rsmd = rs.getMetaData();
		    iColCount = rsmd.getColumnCount();
		    
		    iCount = 0;
			while (rs.next()) {
				ar_sField = new String[iColCount];
			    for (int r=1; r <= iColCount; r++) {
			    	ar_sField[r-1] = rs.getString(r);
			    }
			    if (ar_sField[1] != null) {
			    	ar_sField[1] = ar_sField[1].trim();	
			    }
			    if (ar_sField[2] != null) {
			    	ar_sField[2] = ar_sField[2].trim();	
			    }
			    if (ar_sField[3] != null) {
			    	ar_sField[3] = ar_sField[3].trim();	
			    }
			    mapOfEndUserId.put(ar_sField[0].trim(), ar_sField);
				iCount++;
			}
			
			rs.close();
		}		
		//BP ID, Name, CMR
				if (mapOfBPId.size() > 0) {
					StringBuilder sBufBPId = new StringBuilder((mapOfBPId.size()+1)*13);
					iter = mapOfBPId.keySet().iterator();
					sBufBPId.append('(');
					while (iter.hasNext()) {
						sBufBPId.append('\'');
						sBufBPId.append(iter.next());
						sBufBPId.append("',");
					}
					sBufBPId.setCharAt(sBufBPId.length()-1, ')');

					sQuery = "SELECT MDM.SITE_PARTY_ID, "
							+ "MDM.CUSTNO, "
							+ "RTRIM(MDM.NAME1) || ' ' || RTRIM(MDM.NAME2) "
							+ "FROM WSDIW.MDM_WW_CUSTOMER MDM " +
					"WHERE (MDM.SITE_PARTY_ID IN " + sBufBPId.toString() + ") " +
					"FOR READ ONLY ";
					rs = getDbStatement(false).executeQuery(sQuery);
					rsmd = rs.getMetaData();
				    iColCount = rsmd.getColumnCount();
				    
				    iCount = 0;
					while (rs.next()) {
						ar_sField = new String[iColCount];
					    for (int r=1; r <= iColCount; r++) {
					    	ar_sField[r-1] = rs.getString(r);
					    }
					    if (ar_sField[2] != null) {
					    	ar_sField[2] = ar_sField[2].trim();	
					    }
					    mapOfBPId.put(ar_sField[0].trim(), ar_sField);
						iCount++;
					}
					
					rs.close();
				}			
				
//				
		
		
		if (mapOfQuote.size() > 0) {
			StringBuilder sBufQuotePo = new StringBuilder((mapOfQuote.size()+1)*13);
			iter = mapOfQuote.keySet().iterator();
			sBufQuotePo.append('(');
			while (iter.hasNext()) {
				sBufQuotePo.append('\'');
				sBufQuotePo.append(iter.next());
				sBufQuotePo.append("',");
			}
			sBufQuotePo.setCharAt(sBufQuotePo.length()-1, ')');
			
/*			sQuery = "SELECT RTRIM(CRM.QUOTE_DOC_NO) AS QUOTE_DOC_NO, RTRIM(CRM.QUOTE_DOC_ITEM_NO) AS QUOTE_DOC_ITEM_NO, RTRIM(CRM.PO_NUMBER_SOLD) AS PO_NUMBER_SOLD " +
			"FROM WSDIW.CHSW_WW_QUOTE_ITEM CRM WHERE (CRM.QUOTE_DOC_NO IN " + sBufQuotePo.toString() + ") " +
			"FOR READ ONLY ";*/

			sQuery = "SELECT RTRIM(B.QUOTE_DOC_NO) AS QUOTE_DOC_NO, RTRIM(B.QUOTE_DOC_ITEM_NO) AS QUOTE_DOC_ITEM_NO, " +
					 "RTRIM(B.PO_NUMBER_SOLD) AS PO_NUMBER_SOLD, " +
					 "RTRIM(A.USAGE_TYPE) AS USAGE_TYPE, " +
					 "RTRIM(A.PROCESS_TYPE) AS QUOTE_TYPE, " +
					 "RTRIM(A.SPEC_BID_TYPE) AS SPEC_BID_TYPE_H, " +
					 "RTRIM(B.SPEC_BID_TYPE) AS SPEC_BID_TYPE_I,  " +
					 "A.CREATED_AT AS CREATED_AT,  " +
					 "RTRIM(A.TRANS_DESC) AS TRANS_DESC,  " +
					 "RTRIM(A.OPPOTURNITY_NO) AS OPPOTURNITY_NO  " +
			"FROM WSDIW.CHSW_WW_QUOTE_HEADER A INNER JOIN WSDIW.CHSW_WW_QUOTE_ITEM B ON A.QUOTE_DOC_NO = B.QUOTE_DOC_NO " +
			"WHERE (A.QUOTE_DOC_NO IN " + sBufQuotePo.toString() + ") " +
			"FOR READ ONLY ";
						
			rs = getDbStatement(false).executeQuery(sQuery);
			rsmd = rs.getMetaData();
		    iColCount = rsmd.getColumnCount();
		    String sKey = null;
		    StringTokenizer sTok = null;
		    String sKeyTok = null;
		    iCount = 0;
			while (rs.next()) {
				ar_sField = new String[iColCount];
			    for (int r=1; r <= iColCount; r++) {
			    	ar_sField[r-1] = rs.getString(r);
			    }
			    sKey = mapOfQuoteItem.get(ar_sField[0] + "|" + ar_sField[1]);
			    if (sKey == null) {
			    	continue;
			    }
			    sTok = new StringTokenizer(sKey, "\n");
			    while (sTok.hasMoreTokens()) {
			    	sKeyTok = sTok.nextToken();
				    if (mapOfItem.containsKey(sKeyTok)) {
				    	mapOfItem.get(sKeyTok)[13]=ar_sField[2];
				    	mapOfItem.get(sKeyTok)[22]=ar_sField[3];
				    	mapOfItem.get(sKeyTok)[23]=ar_sField[4];
				    	mapOfItem.get(sKeyTok)[27]=ar_sField[5];
				    	mapOfItem.get(sKeyTok)[28]=ar_sField[6];
				    	mapOfItem.get(sKeyTok)[29]=ar_sField[7];
				    	mapOfItem.get(sKeyTok)[30]=ar_sField[8];
				    	mapOfItem.get(sKeyTok)[31]=ar_sField[9];
				    }
			    }
				iCount++;
			}
			rs.close();
			sBufQuotePo = null;
		}
		
		//***************************************************************************************************
		ar_sField = new String[] {"COUNTRY_CODE",
				"CHANNEL_CODE", "SALES_GROUP", "BILLTO_NO", "BILLTO_NAME", "ORDER_NO", "INVOICE_NO",
				"INVOICE_LINE_AMOUNT", "INVOICE_CRCY", "TIER2_ID", "TIER2_CNAME",  
				"ENDUSER_ID", "ENDUSER_ENAME", "ENDUSER_CNAME", "INST_TYPE", "ENDUSER_ISU", "ENDUSER_COVERAGE",
				"ENDUSER_NAME", "ENDUSER_PHONE", "ENDUSER_ADDRESS", 
				"CONTRACT_CPS_NO", "INVOICE_DATE", "INVOICE_LINE_QTY", "MES", "SDI", "UNIQUEREF", 
				"BP_PURCHASE_ORDER_NO", "SOLDTO_CEID", "BP_CODE", "MATERIAL_NO", "TYPEMODEL", "MACHTYPE", "MACHMODEL", "PROFIT_CENTER", 
				"FEATURE_CODE", "SERIAL_NO", 
				"PROMO_ID", "INVOICE_UNIT_AMOUNT",
				"BID_FLAG", "RR_FLAG", 
				"MIC", "SALES_ORG", "SALESDOC_ORDER_NO", "SALESDOC_ITEM_NO", "PRICE_MATERIAL", "PLANT_ORDER_NO", "BLB",
				//"CANCELLED_BILL", 
				"CANCELLED_BILL_NO", "ORDER_TYPE", "QUOTE_NO", "QUOTE_DOC_ITEM","QUOTE_TYPE", "CHANNEL_ROLE", "SUBMT_ROLE", "ORDER_REASON", "BP_USAGE",
				"BUSINESS_MODEL", "HW_SN_PLANT", "PROD_DIV","SERVICE",
				"T2_ON_ORDER","T2_ON_ORDER_NAME,","SB_OH","SB_OI","SB_QH","SB_QI",
				"ENDUSER_CMR","BILLTO_CEI","BILLTO_ENAME","BILLTO_CMR","SOLDTO_CEI","SOLDTO_ENAME","SOLDTO_CMR","TIER2_CEI","T2_ENAME","T2_CMR",
				"OPPT_ID","ORDER_DATE","QUOTE_DATE","QUOTE_DESC","TA_INDC",
				"CFT_NO","CFT_NAME","CFT_CEI","CFT_ENAME","CFT_CMR","IW_VALID_FROM_TS"}; 

		addRow(ar_sField);
		
		final int COUNTRY_CODE = 0;
		final int CHANNEL_CODE = COUNTRY_CODE + 1;
		final int SALES_GROUP = CHANNEL_CODE + 1;
		final int BILLTO_NO = SALES_GROUP + 1;
		final int BILLTO_NAME = BILLTO_NO + 1;
		final int ORDER_NO = BILLTO_NAME + 1;
		final int INVOICE_NO = ORDER_NO + 1;
		final int INVOICE_LINE_AMOUNT = INVOICE_NO + 1;
		final int INVOICE_CRCY = INVOICE_LINE_AMOUNT + 1;
		final int TIER2_ID = INVOICE_CRCY + 1;
		final int TIER2_CNAME = TIER2_ID + 1;
		final int ENDUSER_ID = TIER2_CNAME + 1;
		final int ENDUSER_ENAME = ENDUSER_ID + 1;
		final int ENDUSER_CNAME = ENDUSER_ENAME + 1;
		final int INST_TYPE = ENDUSER_CNAME + 1;
		final int ENDUSER_ISU = INST_TYPE + 1;
		final int ENDUSER_COVERAGE = ENDUSER_ISU + 1;
		final int ENDUSER_NAME = ENDUSER_COVERAGE + 1;
		final int ENDUSER_PHONE = ENDUSER_NAME + 1;
		final int ENDUSER_ADDRESS = ENDUSER_PHONE + 1;
		final int CONTRACT_CPS_NO = ENDUSER_ADDRESS + 1;
		final int INVOICE_DATE = CONTRACT_CPS_NO + 1;
		final int INVOICE_LINE_QTY = INVOICE_DATE + 1;
		final int MES = INVOICE_LINE_QTY + 1;
		final int SDI = MES + 1;
		final int UNIQUEREF = SDI + 1;
		final int BP_PURCHASE_ORDER_NO = UNIQUEREF + 1;
		final int SOLDTO_CEID = BP_PURCHASE_ORDER_NO + 1;
		final int BP_CODE = SOLDTO_CEID + 1;
		final int MATERIAL_NO = BP_CODE + 1;
		final int TYPEMODEL = MATERIAL_NO + 1;
		final int MACHTYPE = TYPEMODEL + 1;
		final int MACHMODEL = MACHTYPE + 1;
		final int PROFIT_CENTER = MACHMODEL + 1;
		final int FEATURE_CODE = PROFIT_CENTER + 1;
		final int SERIAL_NO = FEATURE_CODE + 1;
		final int PROMO_ID = SERIAL_NO + 1;
		final int INVOICE_UNIT_AMOUNT = PROMO_ID + 1;
		final int BID_FLAG = INVOICE_UNIT_AMOUNT + 1;
		final int RR_FLAG = BID_FLAG + 1;
		final int MIC = RR_FLAG + 1;
		final int SALES_ORG = MIC + 1;
		final int SALESDOC_ORDER_NO = SALES_ORG + 1;
		final int SALESDOC_ITEM_NO = SALESDOC_ORDER_NO + 1;
		final int PRICE_MATERIAL = SALESDOC_ITEM_NO + 1;
		final int PLANT_ORDER_NO = PRICE_MATERIAL + 1;
		final int BLB = PLANT_ORDER_NO + 1;
		final int CANCELLED_BILL_NO = BLB + 1;
		final int SALES_DOC_TYPE = CANCELLED_BILL_NO + 1;
		final int QUOTE_DOC_NO = SALES_DOC_TYPE + 1;
		final int QUOTE_DOC_ITEM = QUOTE_DOC_NO + 1;
		final int QUOTE_TYPE = QUOTE_DOC_ITEM + 1;
		final int CHANNEL_ROLE = QUOTE_TYPE + 1;
		final int SUBMT_ROLE = CHANNEL_ROLE + 1;
		final int ORDER_REASON = SUBMT_ROLE + 1;
		final int USAGE = ORDER_REASON + 1;
		final int BUSINESS_MODEL = USAGE + 1;
		final int HW_SN_PLANT = BUSINESS_MODEL + 1;
		final int PROD_DIV = HW_SN_PLANT + 1;
		final int SERVICE = PROD_DIV + 1;
		final int T2_ON_ORDER = SERVICE + 1;
		final int T2_ON_ORDER_NAME = T2_ON_ORDER + 1;
		final int SB_OH = T2_ON_ORDER_NAME + 1;
		final int SB_OI = SB_OH + 1;
		final int SB_QH = SB_OI + 1;
		final int SB_QI = SB_QH + 1;
		final int ENDUSER_CMR    = SB_QI         + 1;
		final int BILLTO_CEI     = ENDUSER_CMR   + 1;
		final int BILLTO_ENAME   = BILLTO_CEI    + 1;
		final int BILLTO_CMR     = BILLTO_ENAME  + 1;
		final int SOLDTO_CEI     = BILLTO_CMR    + 1;
		final int SOLDTO_ENAME   = SOLDTO_CEI   + 1;
		final int SOLDTO_CMR     = SOLDTO_ENAME  + 1;
		final int TIER2_CEI         = SOLDTO_CMR    + 1;
		final int TIER2_ENAME       = TIER2_CEI        + 1;
		final int TIER2_CMR         = TIER2_ENAME      + 1;
		final int OPPT_ID         = TIER2_CMR      + 1;
		final int ORDER_DATE         = OPPT_ID      + 1;
		final int QUOTE_DATE         = ORDER_DATE      + 1;
		final int QUOTE_DESC         = QUOTE_DATE      + 1;
		final int TA_INDC         = QUOTE_DESC      + 1;
		final int CFT_NO            = TA_INDC      + 1;
		final int CFT_NAME          = CFT_NO         + 1;
		final int CFT_CEI           = CFT_NAME       + 1;
		final int CFT_ENAME         = CFT_CEI        + 1;
		final int CFT_CMR           = CFT_ENAME      + 1;
		final int IW_VALID_FROM_TS  = CFT_CMR      + 1;

		
		Map<String, String> mapOfSalesGroup = new HashMap<String, String>();
		mapOfSalesGroup.put("01|01|01", "A"); // A B C E I N R X Z ...
		mapOfSalesGroup.put("01|01|02", "B");
		mapOfSalesGroup.put("01|01|03", "C"); // C E
		mapOfSalesGroup.put("04|04|04", "D");
		mapOfSalesGroup.put("04|04", "D");
		mapOfSalesGroup.put("04", "D");
		mapOfSalesGroup.put("04|14", "D");
		mapOfSalesGroup.put("04|14|14", "D");
		mapOfSalesGroup.put("06|06|06", "G");
		mapOfSalesGroup.put("06|06", "G");
		mapOfSalesGroup.put("07|07|07", "H");
		mapOfSalesGroup.put("07|07", "H");
		mapOfSalesGroup.put("08|08|08", "J");
		mapOfSalesGroup.put("08|08", "J");
		mapOfSalesGroup.put("10|10|10", "M");
		mapOfSalesGroup.put("10|10", "M");
		mapOfSalesGroup.put("06|14|14", "Q");
		mapOfSalesGroup.put("06|14", "Q");
		mapOfSalesGroup.put("06|13|06", "Y");
		mapOfSalesGroup.put("06|13", "Y");
		
		Map<String, String> mapOfProfitCenter = new HashMap<String, String>();
		mapOfProfitCenter.put("P1006", "P");  
		mapOfProfitCenter.put("P1106", "P");  
		mapOfProfitCenter.put("P1206", "P");  
		mapOfProfitCenter.put("P1306", "P");  
		mapOfProfitCenter.put("P1406", "P");  
		mapOfProfitCenter.put("P1506", "P");  
		mapOfProfitCenter.put("P1606", "P");  
		mapOfProfitCenter.put("P1706", "P");  
		mapOfProfitCenter.put("P1007", "P");  
		mapOfProfitCenter.put("P1107", "P");  
		mapOfProfitCenter.put("P1207", "P");  
		mapOfProfitCenter.put("P1027", "P");  
		mapOfProfitCenter.put("P1127", "P");  
		mapOfProfitCenter.put("P1227", "P");  
		mapOfProfitCenter.put("P1327", "P");  
		mapOfProfitCenter.put("P1427", "P");  
		mapOfProfitCenter.put("P1003", "Z");  
		mapOfProfitCenter.put("P1103", "Z");  
		mapOfProfitCenter.put("P1203", "Z");  
		mapOfProfitCenter.put("P1303", "Z");  
		mapOfProfitCenter.put("P1403", "Z");  
		mapOfProfitCenter.put("P1019", "Z");  
		mapOfProfitCenter.put("P1020", "Z");  
		mapOfProfitCenter.put("P1021", "Z");  
		mapOfProfitCenter.put("P1022", "Z");  
		mapOfProfitCenter.put("P1010", "S");  
		mapOfProfitCenter.put("P1110", "S");  
		mapOfProfitCenter.put("P1011", "S");  
		mapOfProfitCenter.put("P1012", "S");  
		mapOfProfitCenter.put("P1112", "S");  
		mapOfProfitCenter.put("P1212", "S");  
		mapOfProfitCenter.put("P1312", "S");  
		mapOfProfitCenter.put("P1412", "S");  
		mapOfProfitCenter.put("P1512", "S");  
		mapOfProfitCenter.put("P1612", "S");  
		mapOfProfitCenter.put("P1712", "S");  
		mapOfProfitCenter.put("P1812", "S");  
		mapOfProfitCenter.put("P1912", "S");  
		mapOfProfitCenter.put("P1922", "S");  
		mapOfProfitCenter.put("P1028", "S");  
		mapOfProfitCenter.put("P1128", "S");  
		mapOfProfitCenter.put("P1228", "S");  
		mapOfProfitCenter.put("P1328", "S");  
		mapOfProfitCenter.put("P1428", "S");  
		mapOfProfitCenter.put("P1013", "S");  
		mapOfProfitCenter.put("P1014", "S");  
		mapOfProfitCenter.put("P1134", "S");  
		mapOfProfitCenter.put("P1234", "S"); 	
		mapOfProfitCenter.put("P1334", "S"); 	
		mapOfProfitCenter.put("P1434", "S"); 	
		mapOfProfitCenter.put("P1534", "S"); 	
		mapOfProfitCenter.put("P1634", "S"); 	
		mapOfProfitCenter.put("P1734", "S");	
		mapOfProfitCenter.put("P1834", "S");  
		mapOfProfitCenter.put("P1934", "S");  
		mapOfProfitCenter.put("P1018", "S");  
		mapOfProfitCenter.put("P1118", "S");  

		
		boolean isMEBpriced = false;
		Map<String, String> mapOfMEB = new HashMap<String, String>();
		String sNow = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
		int iEqui = 0;
		int iSub = 0;
		String sOrderKey = null;
		String sLastOrderKey = "0000000000|00";
		String sItemKeyZero = null;
		String sQuoteItemKey = null;
		String sBLB = null;
		int q=0;
		String sType = null;
		String sSoldTo = null;
		List<String[]> listOfEquiLoop = null;
		Map<String, Integer> mapOfEquiLoop = null;
		Map<String, String> mapOfEquiMIC = new HashMap<String, String>();
		String sBrand = null;
		Map<String, String> mapOfUsedSN = new HashMap<String, String>();
		String sSNKey = null;
		
		for (int s=0; s<listOfInvoice.size(); s++) {
			try {
				ar_sField[COUNTRY_CODE] = null;
				ar_sField[CHANNEL_CODE] = null;
				ar_sField[SALES_GROUP] = null;
				ar_sField[BILLTO_NO] = null;
				ar_sField[BILLTO_NAME] = null;
				ar_sField[ORDER_NO ] = null;
				ar_sField[INVOICE_NO ] = null;
				ar_sField[INVOICE_LINE_AMOUNT ] = null;
				ar_sField[INVOICE_CRCY ] = null;
				ar_sField[TIER2_ID ] = null;
				ar_sField[TIER2_CNAME ] = null;
				ar_sField[ENDUSER_ID ] = null;
				ar_sField[ENDUSER_ENAME ] = null;
				ar_sField[ENDUSER_CNAME ] = null;
				ar_sField[INST_TYPE ] = null;
				ar_sField[ENDUSER_ISU ] = null;
				ar_sField[ENDUSER_COVERAGE ] = null;
				ar_sField[ENDUSER_NAME ] = null;
				ar_sField[ENDUSER_PHONE ] = null;
				ar_sField[ENDUSER_ADDRESS ] = null;
				ar_sField[CONTRACT_CPS_NO ] = null;
				ar_sField[INVOICE_DATE ] = null;
				ar_sField[INVOICE_LINE_QTY ] = null;
				ar_sField[MES ] = null;
				ar_sField[SDI ] = null;
				ar_sField[UNIQUEREF ] = null;
				ar_sField[BP_PURCHASE_ORDER_NO ] = null;
				ar_sField[SOLDTO_CEID ] = null;
				ar_sField[BP_CODE ] = null;
				ar_sField[MATERIAL_NO ] = null;
				ar_sField[TYPEMODEL ] = null;
				ar_sField[MACHTYPE ] = null;
				ar_sField[MACHMODEL ] = null;
				ar_sField[PROFIT_CENTER ] = null;
				ar_sField[FEATURE_CODE ] = null;
				ar_sField[SERIAL_NO ] = null;
				ar_sField[PROMO_ID ] = null;
				ar_sField[INVOICE_UNIT_AMOUNT ] = null;
				ar_sField[BID_FLAG ] = null;
				ar_sField[RR_FLAG ] = null;
				ar_sField[MIC ] = null;
				ar_sField[SALES_ORG ] = null;
				ar_sField[SALESDOC_ORDER_NO ] = null;
				ar_sField[SALESDOC_ITEM_NO ] = null;
				ar_sField[PRICE_MATERIAL ] = null;
				ar_sField[PLANT_ORDER_NO ] = null;
				ar_sField[BLB ] = null;
				ar_sField[CANCELLED_BILL_NO ] = null;
				ar_sField[SALES_DOC_TYPE ] = null;
				ar_sField[QUOTE_DOC_NO ] = null;
				ar_sField[QUOTE_DOC_ITEM ] = null;
				ar_sField[QUOTE_TYPE ] = null;
				ar_sField[CHANNEL_ROLE ] = null;
				ar_sField[SUBMT_ROLE ] = null;
				ar_sField[ORDER_REASON ] = null;
				ar_sField[USAGE ] = null;
				ar_sField[BUSINESS_MODEL ] = null;
				ar_sField[HW_SN_PLANT ] = null;
				ar_sField[PROD_DIV ] = null;
				ar_sField[T2_ON_ORDER ] = null;
				ar_sField[T2_ON_ORDER_NAME ] = null;
				ar_sField[SERVICE ] = null;
				ar_sField[SB_OH] = null;
				ar_sField[SB_OI ] = null;
				ar_sField[SB_QH ] = null;
				ar_sField[SB_QI ] = null;
				ar_sField[ENDUSER_CMR ]   = null;
				ar_sField[BILLTO_CEI ]   = null;
				ar_sField[BILLTO_ENAME]   = null;
				ar_sField[BILLTO_CMR  ]   = null;
				ar_sField[SOLDTO_CEI ]   = null;
				ar_sField[SOLDTO_ENAME]   = null;
				ar_sField[SOLDTO_CMR  ]   = null;
				ar_sField[TIER2_CEI     ]   = null;
				ar_sField[TIER2_ENAME    ]   = null;
				ar_sField[TIER2_CMR      ]   = null;
				ar_sField[OPPT_ID]   = null;
				ar_sField[ORDER_DATE  ]   = null;
				ar_sField[QUOTE_DATE     ]   = null;
				ar_sField[QUOTE_DESC    ]   = null;
				ar_sField[TA_INDC      ]   = null;
				ar_sField[CFT_NO  ]    = null;   
				ar_sField[CFT_NAME ]   = null;   
				ar_sField[CFT_CEI  ]   = null;   
				ar_sField[CFT_ENAME  ]   = null; 
				ar_sField[CFT_CMR     ]   = null;
				ar_sField[IW_VALID_FROM_TS     ]   = null;
				

				
//				if ((sLastBillDate!=null) && (listOfInvoice.get(s)[7]!=null) && (listOfInvoice.get(s)[7].compareTo(sLastBillDate)<0)) {
//					continue;
//				}
//
//				if ((sEndBillDate!=null) && (listOfInvoice.get(s)[7]!=null) && (listOfInvoice.get(s)[7].compareTo(sEndBillDate)>0)) {
//					continue;
//				}
				
				ar_sField[MATERIAL_NO]=listOfInvoice.get(s)[4];
				if ((ar_sField[MATERIAL_NO]!=null) && (ar_sField[MATERIAL_NO].startsWith("69SA")) ) {//Bottom line BIDs 
                    continue;
				}
				
				ar_sField[ORDER_NO]=listOfInvoice.get(s)[1]; //sales order number
//				if (!mapOfOrderNo.containsKey(ar_sField[ORDER_NO])) {
//					continue;
//				}

				sOrderKey = listOfInvoice.get(s)[1] + "|" + listOfInvoice.get(s)[2];
				
				if (mapOfItem.containsKey(sOrderKey)) {
					ar_sField[BLB]=mapOfItem.get(sOrderKey)[5];
					if ((ar_sField[BLB] != null) && (ar_sField[BLB].equals("Y"))) {
						ar_sField[BLB] = "X";
					} else {
						ar_sField[BLB] = "";
					}
				}
				
				if((ar_sField[BLB] == "") && ((listOfInvoice.get(s)[24].equals("ZF2I") || listOfInvoice.get(s)[2].equals("ZS1I")))){
					continue;
				}
				//sales order type
				if (mapOfOrderNo.containsKey(ar_sField[ORDER_NO])) {
					ar_sField[ORDER_DATE]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[1];
					ar_sField[SALES_DOC_TYPE]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[3];
					ar_sField[ORDER_REASON]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[4];
					ar_sField[QUOTE_DOC_NO]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[6];
					ar_sField[CHANNEL_ROLE]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[7];
					ar_sField[SUBMT_ROLE]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[8];
					if((listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[10].equals("X")) || (listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[11].equals("X"))){
						ar_sField[SB_OH] = "X";
					}else{
						ar_sField[SB_OH] = "N";
					}
				}
				//get Sales group from sales order rather than billing
				ar_sField[SALES_GROUP]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[5];
				ar_sField[CHANNEL_CODE]=mapOfSalesGroup.get(ar_sField[SALES_GROUP] + "|" + ar_sField[CHANNEL_ROLE] + "|" + ar_sField[SUBMT_ROLE]);
				if ((ar_sField[CHANNEL_CODE]==null) || (ar_sField[CHANNEL_CODE].trim().length()==0)) {
					ar_sField[CHANNEL_CODE]=mapOfSalesGroup.get(ar_sField[SALES_GROUP] + "|" + ar_sField[CHANNEL_ROLE] );
				}
				/*				ar_sField[SALES_GROUP]=listOfInvoice.get(s)[17];
				if ((ar_sField[SALES_GROUP]==null) || (ar_sField[SALES_GROUP].trim().length()==0)) {
					if (mapOfOrderNo.containsKey(ar_sField[ORDER_NO])) {
						ar_sField[SALES_GROUP]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[5];
					}
					ar_sField[CHANNEL_CODE]=mapOfSalesGroup.get(ar_sField[SALES_GROUP]);
				}*/
				if (ar_sField[CHANNEL_CODE] != null){
					if (ar_sField[CHANNEL_CODE].equals("Q") || ar_sField[CHANNEL_CODE].equals("D")) {
						ar_sField[BUSINESS_MODEL]= "O";
					}
				}


			
				//Usage exclude BP demo and BP use only
//				if (mapOfItem.containsKey(sOrderKey)) {
//					if ((mapOfItem.get(sOrderKey)[22] != null) && (mapOfItem.get(sOrderKey)[22].trim().length()>0)){
//						if ((mapOfItem.get(sOrderKey)[22].equals("BP2")) || (mapOfItem.get(sOrderKey)[22].equals("BP3"))){
//							continue;
//						}
//					}
//				}
				//add usage field
				if (mapOfItem.containsKey(sOrderKey)) {
					ar_sField[USAGE] = mapOfItem.get(sOrderKey)[22];
					ar_sField[QUOTE_TYPE] = mapOfItem.get(sOrderKey)[23];
					ar_sField[QUOTE_DOC_NO] = mapOfItem.get(sOrderKey)[11];
					ar_sField[QUOTE_DOC_ITEM] = mapOfItem.get(sOrderKey)[12];
					ar_sField[SB_OI] = mapOfItem.get(sOrderKey)[26];
					ar_sField[SB_QH] = mapOfItem.get(sOrderKey)[27];
					ar_sField[SB_QI] = mapOfItem.get(sOrderKey)[28];
					ar_sField[QUOTE_DATE] = mapOfItem.get(sOrderKey)[29];
					ar_sField[QUOTE_DESC] = mapOfItem.get(sOrderKey)[30];
					ar_sField[OPPT_ID] = mapOfItem.get(sOrderKey)[31];
					if (ar_sField[SB_OI] == null){
						ar_sField[SB_OI] = "N";
					}
					if (ar_sField[SB_QH] == null){
						ar_sField[SB_QH] = "N";
					}
					if (ar_sField[SB_QI] == null){
						ar_sField[SB_QI] = "N";
					}
				}
				if (mapOfQuoteText.containsKey(ar_sField[QUOTE_DOC_NO])) {
					ar_sField[TA_INDC] = "X";
				}
				if (mapOfItemDetail.containsKey(sOrderKey)) {
					ar_sField[PROD_DIV] = mapOfItemDetail.get(sOrderKey);
				}
				if (ar_sField[USAGE].equals("BP3")) {
					ar_sField[BUSINESS_MODEL] = "D";
				}
				if (ar_sField[USAGE].equals("BP2")) {
					ar_sField[BUSINESS_MODEL] = "U";
				}
				
				ar_sField[COUNTRY_CODE]=listOfInvoice.get(s)[16];
				sSoldTo = listOfInvoice.get(s)[18];

				
// T2 on order
				ar_sField[T2_ON_ORDER] = null;
				ar_sField[T2_ON_ORDER_NAME] = null;
				if (mapOfSADR.containsKey(ar_sField[ORDER_NO])) {
					ar_sField[T2_ON_ORDER]=getLegacyCeid(mapOfSADR.get(ar_sField[ORDER_NO])[1]);
					ar_sField[T2_ON_ORDER_NAME]=mapOfSADR.get(ar_sField[ORDER_NO])[2];
				}
//				
				
				//sItemKeyZero = listOfInvoice.get(s)[1] + "|0";
				sItemKeyZero = ar_sField[QUOTE_DOC_NO] + "|0";
				if (mapOfAddress.containsKey(sItemKeyZero)) {
					iSub = mapOfAddress.get(sItemKeyZero);
					while ((iSub < listOfAddress.size()) && (sItemKeyZero.equals(listOfAddress.get(iSub)[0] + "|0"))) {
						if (listOfAddress.get(iSub)[2].startsWith("00000003")) {
							ar_sField[BILLTO_NAME]=listOfAddress.get(iSub)[4];
							if ((ar_sField[BILLTO_NO]==null) || (ar_sField[BILLTO_NO].trim().length()==0)) {
								ar_sField[BILLTO_NO]=getLegacyCeid(listOfAddress.get(iSub)[3]);
							}
							ar_sField[BILLTO_CEI]=listOfAddress.get(iSub)[3];
							if(mapOfBPId.containsKey(ar_sField[BILLTO_CEI])) {
								ar_sField[BILLTO_ENAME] = mapOfBPId.get(ar_sField[BILLTO_CEI])[2];
								ar_sField[BILLTO_CMR] = mapOfBPId.get(ar_sField[BILLTO_CEI])[1];
							}
						}else
							if (listOfAddress.get(iSub)[2].startsWith("00000001")) {
								ar_sField[SOLDTO_CEI]=listOfAddress.get(iSub)[3];
								if(mapOfBPId.containsKey(ar_sField[SOLDTO_CEI])) {
									ar_sField[SOLDTO_ENAME] = mapOfBPId.get(ar_sField[SOLDTO_CEI])[2];
									ar_sField[SOLDTO_CMR] = mapOfBPId.get(ar_sField[SOLDTO_CEI])[1];
								}
							}
						else
							if (listOfAddress.get(iSub)[2].startsWith("ZCT")) {
								ar_sField[CFT_NAME]=listOfAddress.get(iSub)[4];
								if ((ar_sField[CFT_NO]==null) || (ar_sField[CFT_NO].trim().length()==0)) {
									ar_sField[CFT_NO]=getLegacyCeid(listOfAddress.get(iSub)[3]);
								}
								ar_sField[CFT_CEI]=listOfAddress.get(iSub)[3];
								if(mapOfBPId.containsKey(ar_sField[CFT_CEI])) {
									ar_sField[CFT_ENAME] = mapOfBPId.get(ar_sField[CFT_CEI])[2];
									ar_sField[CFT_CMR] = mapOfBPId.get(ar_sField[CFT_CEI])[1];
							    }
							}	
						else
						if (listOfAddress.get(iSub)[2].startsWith("ZCO")) {
							ar_sField[ENDUSER_NAME]=listOfAddress.get(iSub)[4];
						} else
						if (listOfAddress.get(iSub)[2].startsWith("ZTC")) {
							ar_sField[TIER2_ID]=getLegacyCeid(listOfAddress.get(iSub)[3]);
							ar_sField[TIER2_CNAME]=listOfAddress.get(iSub)[4];
							
							ar_sField[TIER2_CEI]=listOfAddress.get(iSub)[3];
							if(mapOfBPId.containsKey(ar_sField[TIER2_CEI])) {
								try{
									ar_sField[TIER2_ENAME] = mapOfBPId.get(ar_sField[TIER2_CEI])[2];
									ar_sField[TIER2_CMR] = mapOfBPId.get(ar_sField[TIER2_CEI])[1];
								}catch (Exception nfe) {
								}

							}	
						}
						iSub++;
					}
				}
			
				
				//override with line info if available
/*				if (mapOfAddress.containsKey(sOrderKey)) {
					iSub = mapOfAddress.get(sOrderKey);
					while ((iSub < listOfAddress.size()) && (sOrderKey.equals(listOfAddress.get(iSub)[0] + "|" + listOfAddress.get(iSub)[1]))) {
						if (listOfAddress.get(iSub)[2].startsWith("00000003")) {
							ar_sField[BILLTO_NAME]=listOfAddress.get(iSub)[4];
							if ((ar_sField[BILLTO_NO]==null) || (ar_sField[BILLTO_NO].trim().length()==0)) {
								ar_sField[BILLTO_NO]=getLegacyCeid(listOfAddress.get(iSub)[3]);
							}
						} else
						if (listOfAddress.get(iSub)[2].startsWith("ZCO")) {
							ar_sField[ENDUSER_NAME]=listOfAddress.get(iSub)[4];
						} else
						if (listOfAddress.get(iSub)[2].startsWith("ZTC")) {
							ar_sField[TIER2_ID]=getLegacyCeid(listOfAddress.get(iSub)[3]);
							ar_sField[TIER2_CNAME]=listOfAddress.get(iSub)[4];
						}
						iSub++;
					}
				}*/

				sQuoteItemKey = ar_sField[QUOTE_DOC_NO] + "|" +ar_sField[QUOTE_DOC_ITEM];
				if ((ar_sField[ENDUSER_ID]==null) && (mapOfAddressCRM.containsKey(sQuoteItemKey))) {
					ar_sField[ENDUSER_ID]=mapOfAddressCRM.get(sQuoteItemKey)[3];
					ar_sField[ENDUSER_CNAME]=mapOfAddressCRM.get(sQuoteItemKey)[4];
					ar_sField[ENDUSER_ENAME]=mapOfAddressCRM.get(sQuoteItemKey)[6];
					ar_sField[ENDUSER_PHONE]=mapOfAddressCRM.get(sQuoteItemKey)[8];
					ar_sField[ENDUSER_ADDRESS]=mapOfAddressCRM.get(sQuoteItemKey)[9];
				}
				
				if ((ar_sField[ENDUSER_ID]==null) && (mapOfAddressCRM.containsKey(sItemKeyZero))) {
					ar_sField[ENDUSER_ID]=mapOfAddressCRM.get(sItemKeyZero)[3];
					ar_sField[ENDUSER_CNAME]=mapOfAddressCRM.get(sItemKeyZero)[4];
					ar_sField[ENDUSER_ENAME]=mapOfAddressCRM.get(sItemKeyZero)[6];
					ar_sField[ENDUSER_PHONE]=mapOfAddressCRM.get(sItemKeyZero)[8];
					ar_sField[ENDUSER_ADDRESS]=mapOfAddressCRM.get(sItemKeyZero)[9];
			    }
				
				ar_sField[INST_TYPE]=null;
				if (mapOfItem.containsKey(sOrderKey)) {
					ar_sField[INST_TYPE]=mapOfItem.get(sOrderKey)[10];
				}
				ar_sField[MES]=null;
				ar_sField[SERIAL_NO]=null;
				if (mapOfItem.containsKey(sOrderKey)) {
					if (mapOfItem.get(sOrderKey)[16].equals("UPG")){
					   ar_sField[MES]="X";
					   ar_sField[SERIAL_NO]=mapOfItem.get(sOrderKey)[17];
					}
				}
				if ((ar_sField[ENDUSER_ID]!=null) && (mapOfEndUserId.containsKey(ar_sField[ENDUSER_ID]))) {
					if (mapOfEndUserId.get(ar_sField[ENDUSER_ID])!=null) {
						ar_sField[ENDUSER_ISU] = mapOfEndUserId.get(ar_sField[ENDUSER_ID])[1];
						ar_sField[ENDUSER_COVERAGE] = mapOfEndUserId.get(ar_sField[ENDUSER_ID])[2];
						ar_sField[ENDUSER_ENAME] = mapOfEndUserId.get(ar_sField[ENDUSER_ID])[3];
						ar_sField[ENDUSER_CMR] = mapOfEndUserId.get(ar_sField[ENDUSER_ID])[4];
					}
				}

				ar_sField[INVOICE_NO]=listOfInvoice.get(s)[8];
				ar_sField[INVOICE_LINE_AMOUNT]=listOfInvoice.get(s)[10];
				ar_sField[INVOICE_CRCY]=listOfInvoice.get(s)[11];

				ar_sField[INVOICE_DATE]=listOfInvoice.get(s)[7];
				ar_sField[INVOICE_LINE_QTY]=listOfInvoice.get(s)[12];

				if ((ar_sField[MATERIAL_NO]!=null) && (ar_sField[MATERIAL_NO].trim().length()==7) && (ar_sField[MATERIAL_NO].substring(4,7).equalsIgnoreCase("MEB"))) {
					mapOfMEB.put(listOfInvoice.get(s)[1], null);
				}
//				if (mapOfMEB.containsKey(listOfInvoice.get(s)[1])) {
//					ar_sField[MES]="X";
//				}

				ar_sField[UNIQUEREF]=null;
				ar_sField[BP_PURCHASE_ORDER_NO]=null;
				if (mapOfItem.containsKey(sOrderKey)) {
					ar_sField[BP_PURCHASE_ORDER_NO]=mapOfItem.get(sOrderKey)[13];
				}
				if ((ar_sField[BP_PURCHASE_ORDER_NO] == null) || (ar_sField[BP_PURCHASE_ORDER_NO].length()==0)) {
					if (mapOfOrderNo.containsKey(ar_sField[ORDER_NO])) {
						ar_sField[BP_PURCHASE_ORDER_NO]=listOfOrder.get(Integer.parseInt(mapOfOrderNo.get(ar_sField[ORDER_NO])))[2];
					}
				}
				ar_sField[SOLDTO_CEID]=listOfInvoice.get(s)[0];
				ar_sField[SOLDTO_CEID]=getLegacyCeid(ar_sField[SOLDTO_CEID]);
				
				if ((ar_sField[BP_PURCHASE_ORDER_NO] != null) && (ar_sField[BP_PURCHASE_ORDER_NO].length()>3)) {
					ar_sField[BP_CODE]=ar_sField[BP_PURCHASE_ORDER_NO].substring(0, 4);
				} else {
					ar_sField[BP_CODE]=null;
				}

				if (mapOfItem.containsKey(sOrderKey)) { //MTM
					ar_sField[TYPEMODEL]=mapOfItem.get(sOrderKey)[3];
				}

				sBrand = null;
				if (mapOfItem.containsKey(sOrderKey)) {
					if (((ar_sField[MACHTYPE]==null) || (ar_sField[MACHTYPE].trim().length()==0)) && ((ar_sField[MATERIAL_NO]!=null) && (ar_sField[MATERIAL_NO].trim().length()>6))) {
						ar_sField[MACHTYPE]=ar_sField[MATERIAL_NO].substring(0,4);				
					}
					ar_sField[MACHMODEL]=mapOfItem.get(sOrderKey)[7]; 

					if (((ar_sField[MACHMODEL]==null) || (ar_sField[MACHMODEL].trim().length()==0))) {
						if (mapOfFeaturesModel.containsKey(sOrderKey)){
							ar_sField[MACHMODEL]=mapOfFeaturesModel.get(sOrderKey)[3];
						}
						if (((ar_sField[MACHMODEL]==null) || (ar_sField[MACHMODEL].trim().length()==0))) {
							ar_sField[MACHMODEL]=ar_sField[MATERIAL_NO].substring(4,7);
						}
					}
					ar_sField[PROFIT_CENTER]=mapOfItem.get(sOrderKey)[4];
					sBrand = mapOfMaterial.get(ar_sField[PROFIT_CENTER]); 
				}
				if ((ar_sField[TYPEMODEL]==null) || (ar_sField[TYPEMODEL].trim().length()==0)) {
					ar_sField[TYPEMODEL]=ar_sField[MACHTYPE] + ar_sField[MACHMODEL];				
				}
				if(ar_sField[PROFIT_CENTER] != null) {
					sBrand = mapOfProfitCenter.get(ar_sField[PROFIT_CENTER]);
					if ((sBrand == "P") && ( sBrand != null )){
						if (mapOfFeaturesSDI.containsKey(ar_sField[ORDER_NO])) {
							ar_sField[SDI]="X";
						}
					}
//					if ((ar_sField[PROFIT_CENTER].equals("P1003")) || (ar_sField[PROFIT_CENTER].equals("P1019")) || (ar_sField[PROFIT_CENTER].equals("P1020")) 
//							|| (ar_sField[PROFIT_CENTER].equals("P1021")) || (ar_sField[PROFIT_CENTER].equals("P1022")) ){
//						continue;	
//					}
				}
				//SDI logic
				sBrand = mapOfProfitCenter.get(ar_sField[PROFIT_CENTER]);
				if ((sBrand == "P") && ( sBrand != null )){
					if (mapOfFeaturesSDI.containsKey(ar_sField[ORDER_NO])) {
						ar_sField[SDI]="X";
					}
				}
				ar_sField[FEATURE_CODE]=null;
//				if ((ar_sField[MATERIAL_NO]!=null) && (ar_sField[MATERIAL_NO].length()>7) && (ar_sField[MATERIAL_NO].charAt(4) == 'F')) {
//					ar_sField[FEATURE_CODE]=ar_sField[MATERIAL_NO].substring(5);
//				}
				
				ar_sField[PROMO_ID]=null;
//				if (mapOfItem.containsKey(sOrderKey)) {
//					ar_sField[PROMO_ID]=mapOfItem.get(sOrderKey)[2];
//				}
//				if ((ar_sField[PROMO_ID]==null) || (ar_sField[PROMO_ID].trim().length()==0)) {
//					ar_sField[PROMO_ID]="NOR";
//				}
				if ((ar_sField[ORDER_REASON]!=null) && (ar_sField[ORDER_REASON].equals("B50") || ar_sField[ORDER_REASON].equals("B51"))) {
					ar_sField[PROMO_ID]="Grid_Pricing";
				}

				
				ar_sField[INVOICE_UNIT_AMOUNT]=listOfInvoice.get(s)[13];
				
				ar_sField[BID_FLAG]=null;
				if ((ar_sField[PROMO_ID]!=null) && (!ar_sField[PROMO_ID].equals("NOR"))) {
					ar_sField[BID_FLAG] = "A";
				} else {
					if ((mapOfItem.containsKey(sOrderKey)) && (mapOfItem.get(sOrderKey)[9] != null) && (mapOfItem.get(sOrderKey)[9].trim().length()>0)) {
						ar_sField[BID_FLAG] = "B";
					} else {
						ar_sField[BID_FLAG] = "C";
					}
				}
				
				
				ar_sField[RR_FLAG]=null;				

				ar_sField[CONTRACT_CPS_NO]=null;
				ar_sField[CONTRACT_CPS_NO]=listOfInvoice.get(s)[3];
/*				if ((ar_sField[BUSINESS_MODEL]==null) || (ar_sField[BUSINESS_MODEL].trim().length()==0)) {
					if (ar_sField[QUOTE_TYPE].equals("ZBPQ")){
						ar_sField[RR_FLAG] = "Y";
						ar_sField[BUSINESS_MODEL] = "R";
					}else if( (ar_sField[QUOTE_TYPE].equals("ZQT1")) && 
							((ar_sField[ENDUSER_ID]==null) || (ar_sField[ENDUSER_ID].trim().length()==0) || (!ar_sField[ENDUSER_ID].startsWith("S")) ) ){
						ar_sField[RR_FLAG] = "Y";
						ar_sField[BUSINESS_MODEL] = "R";
					}
					else if((ar_sField[QUOTE_TYPE].equals("ZQT1")) && (ar_sField[ENDUSER_ID].startsWith("S")) && (!ar_sField[CONTRACT_CPS_NO].startsWith("83"))){
						ar_sField[RR_FLAG] = "N";
						ar_sField[BUSINESS_MODEL] = "A";
					} 
					else{
						ar_sField[RR_FLAG] = "N";
						ar_sField[BUSINESS_MODEL] = "B";
					}
				}*/
				if ((ar_sField[BUSINESS_MODEL]==null) || (ar_sField[BUSINESS_MODEL].trim().length()==0)) {
					if (ar_sField[QUOTE_TYPE].equals("ZBPQ")){
						ar_sField[RR_FLAG] = "Y";
						ar_sField[BUSINESS_MODEL] = "R";
					}
					else if( (ar_sField[QUOTE_TYPE].equals("ZQT1")) && 
							((ar_sField[ENDUSER_ID]==null) || (ar_sField[ENDUSER_ID].trim().length()==0) || (!ar_sField[ENDUSER_ID].startsWith("S")) ) ){
						ar_sField[RR_FLAG] = "Y";
						ar_sField[BUSINESS_MODEL] = "R";
					}
					else if((ar_sField[QUOTE_TYPE].equals("ZQT1")) && (ar_sField[ENDUSER_ID].startsWith("S")) && (ar_sField[CONTRACT_CPS_NO].startsWith("83"))){
						ar_sField[RR_FLAG] = "N";
						ar_sField[BUSINESS_MODEL] = "B";
					} 
					else if((ar_sField[SB_OH].equals("X")) || (ar_sField[SB_OI].equals("X")) || (ar_sField[SB_QH].equals("X")) || (ar_sField[SB_QI].equals("X"))){
						ar_sField[RR_FLAG] = "N";
						ar_sField[BUSINESS_MODEL] = "B";
					}
					else{
						ar_sField[RR_FLAG] = "N";
						ar_sField[BUSINESS_MODEL] = "A";
					}
				}
					
				// Transactions without EU will be considered Run Rate
//				if (((ar_sField[ENDUSER_ID]==null) || (ar_sField[ENDUSER_ID].trim().length()==0)) && (ar_sField[RR_FLAG] != null)) {
//					ar_sField[RR_FLAG] = "Y";
//				} else {
//					ar_sField[RR_FLAG] = "N";
//				}
				
				ar_sField[MIC]=null;
				if (mapOfFeaturesMIC.containsKey(ar_sField[ORDER_NO])) {
					ar_sField[MIC]="X";
				} 
				if (ar_sField[MIC] == null) {
					ar_sField[MIC] = "";
				}
				ar_sField[SALES_ORG]=listOfInvoice.get(s)[16];
				ar_sField[SALESDOC_ORDER_NO]=listOfInvoice.get(s)[1];
				ar_sField[SALESDOC_ITEM_NO]=listOfInvoice.get(s)[2];
				ar_sField[PRICE_MATERIAL]=ar_sField[MATERIAL_NO];
				ar_sField[PLANT_ORDER_NO]=null; 
				
				
				ar_sField[CANCELLED_BILL_NO]=listOfInvoice.get(s)[22];
				if (ar_sField[CANCELLED_BILL_NO] == null) {
					ar_sField[CANCELLED_BILL_NO] = "";
				}
				
				ar_sField[IW_VALID_FROM_TS]=listOfInvoice.get(s)[26];
				
				if (mapOfEqui.containsKey(sOrderKey)){
					mapOfEquiLoop = mapOfEqui;
					listOfEquiLoop = listOfEqui;

					iEqui = mapOfEquiLoop.get(sOrderKey);
					int iEquiQty = iEqui + m_nfUS.parse(listOfInvoice.get(s)[12]).intValue(); //limit up to billing quantity 
					while ((iEqui < listOfEquiLoop.size()) && (iEqui < iEquiQty)
							&& (sOrderKey.equals(listOfEquiLoop.get(iEqui)[0] + "|" + listOfEquiLoop.get(iEqui)[1]))) {
/*						if((ar_sField[BLB] != null) && (ar_sField[BLB].equals("X"))){
							ar_sField[PLANT_ORDER_NO]=listOfEquiLoop.get(iEqui)[4];							
							ar_sField[INVOICE_LINE_QTY] = "1";
							ar_sField[INVOICE_LINE_AMOUNT] = ar_sField[INVOICE_UNIT_AMOUNT];
							
							if(!ar_sField[MATERIAL_NO].substring(4).trim().toUpperCase().equals("MEB")){
							  ar_sField[UNIQUEREF]=sNow + formatCount();
							  ar_sField[MATERIAL_NO] = ar_sField[TYPEMODEL];	
							  ar_sField[SERIAL_NO]=listOfEquiLoop.get(iEqui)[3];
							  addRow(ar_sField);
							}
							iEqui++;
							continue;
						}*/
						
						if (listOfInvoice.get(s)[7].compareTo(listOfEquiLoop.get(iEqui)[5])>=0){
							sSNKey = listOfEquiLoop.get(iEqui)[3] + "|" + listOfEquiLoop.get(iEqui)[0] + "|" + listOfEquiLoop.get(iEqui)[1] + "|" + ar_sField[PRICE_MATERIAL] + "|" + listOfInvoice.get(s)[21];
							if (mapOfUsedSN.containsKey(sSNKey)){
								iEqui++;
								iEquiQty++;
								continue;
							}
						
							ar_sField[PLANT_ORDER_NO]=listOfEquiLoop.get(iEqui)[4];							
							ar_sField[INVOICE_LINE_QTY] = "1";
							ar_sField[INVOICE_LINE_AMOUNT] = ar_sField[INVOICE_UNIT_AMOUNT];
							
							if(!ar_sField[MATERIAL_NO].substring(4).trim().toUpperCase().equals("MEB")){
							  ar_sField[UNIQUEREF]=sNow + formatCount();
							  ar_sField[MATERIAL_NO] = ar_sField[TYPEMODEL];	
							  ar_sField[SERIAL_NO]=listOfEquiLoop.get(iEqui)[3];
							  addRow(ar_sField);
							  mapOfUsedSN.put(sSNKey, listOfEquiLoop.get(iEqui)[0] + "|" + listOfEquiLoop.get(iEqui)[1] + "|" + listOfEquiLoop.get(iEqui)[5]);
							}
							iEqui++;
						}
						else{	
							iEqui++;	
							iEquiQty++;
						}					
					} // while S/N as per invoice quantity
				} 

				//begin of block------------------------
				if ((ar_sField[MATERIAL_NO]!=null) && (ar_sField[MATERIAL_NO].trim().length()==7)) {
					sType = ar_sField[MATERIAL_NO].substring(4).trim().toUpperCase();
					if (sType.equals("MEB")) {
							
							//if (ar_sField[MIC]==null) {

								iSub = mapOfFeaturesItem.get(sOrderKey);
								while ((iSub < listOfFeatures.size()) && (sOrderKey.equals(listOfFeatures.get(iSub)[0] + "|" + listOfFeatures.get(iSub)[1]))) {
                                    if ( (listOfFeatures.get(iSub)[4].startsWith("0003") ) || (listOfFeatures.get(iSub)[4].startsWith("5555") ) ){
                                    	iSub++;
                                    	continue;
                                    }
                                    ar_sField[INVOICE_LINE_QTY] = "1";
                                	ar_sField[MATERIAL_NO]  = listOfInvoice.get(s)[4].substring(0, 4) + "F" + listOfFeatures.get(iSub)[4] ;
									ar_sField[FEATURE_CODE] = listOfFeatures.get(iSub)[4];
									//feature installation type
									ar_sField[INST_TYPE]    = mapOfFeatureItem.get(ar_sField[SALESDOC_ORDER_NO] + "|" + ar_sField[SALESDOC_ITEM_NO] + "|" + ar_sField[MATERIAL_NO].trim());
                                    if  (ar_sField[MIC].equals("X"))  {
                                    	if (listOfFeatures.get(iSub)[4].startsWith("ECS0")){
                                    		ar_sField[INVOICE_UNIT_AMOUNT] = ar_sField[INVOICE_LINE_AMOUNT];
        									ar_sField[UNIQUEREF]=sNow + formatCount();							
    										addRow(ar_sField);
    										iSub++;
    										continue;
                                    	}else {
                                            //int p = m_nfUS.parse(listOfFeatures.get(iSub)[5]).intValue();
        									for (int r=0; r<m_nfUS.parse(listOfFeatures.get(iSub)[5]).intValue(); r++) {
        										ar_sField[INVOICE_UNIT_AMOUNT] = String.valueOf("0");
        										ar_sField[UNIQUEREF]=sNow + formatCount();							
        										addRow(ar_sField);
        									}
                                    	}
                                    }else {
                                        int p = m_nfUS.parse(listOfFeatures.get(iSub)[5]).intValue();
    									for (int r=0; r<p; r++) {
    										try {
												ar_sField[INVOICE_UNIT_AMOUNT] = String.valueOf(Double.parseDouble(ar_sField[INVOICE_LINE_AMOUNT]) / Double.parseDouble(listOfFeatures.get(iSub)[5]));
											} catch (Exception nfe) {
												ar_sField[INVOICE_UNIT_AMOUNT] = null;
											}
    										ar_sField[UNIQUEREF]=sNow + formatCount();							
    										addRow(ar_sField);
    									}
    									break;
                                    }

									iSub++;
								}

							//} 

					} // else MEB
				} // NEW / UPG

                
				//end of block ---------------------------------------------
                if ((ar_sField[SALESDOC_ORDER_NO]!=null) && (ar_sField[SALESDOC_ORDER_NO].startsWith("73"))) {
					ar_sField[INVOICE_LINE_QTY] = "1";
					ar_sField[UNIQUEREF]=sNow + formatCount();							
					addRow(ar_sField);
				}
				
				if ((ar_sField[ORDER_NO]!=null) && (ar_sField[ORDER_NO].startsWith("70"))){
					ar_sField[INVOICE_LINE_QTY] = "1";
					ar_sField[UNIQUEREF]=sNow + formatCount();
					ar_sField[SERVICE]="X";
				    if (mapOfContractItem.containsKey(sOrderKey)) {
				    	String[] array_sField = null;
				    	array_sField = mapOfContractItem.get(sOrderKey);
				    	if (mapOfSNPlant.containsKey(array_sField[6] + "|" +array_sField[7] + "|" + array_sField[8])){
				    		if (array_sField[8] != null){
				    			ar_sField[HW_SN_PLANT] = array_sField[8] + "-" + mapOfSNPlant.get(array_sField[6] + "|" +array_sField[7] + "|" + array_sField[8]);
				    		}				    		
				    	}
				    	else{
				    		if (array_sField[8] != null){
				    			ar_sField[HW_SN_PLANT] = array_sField[8];
				    		}	
				    	}
				    	
				    }
					addRow(ar_sField);
				}
				setLastIwTs(listOfInvoice.get(s)[19]);

			
			} catch (NumberFormatException nfe) {}
			sLastOrderKey = sOrderKey;
		
		} // for listOfInvoice
		
		//------------------------------		
	}
	
	private String formatCount() {
		m_lCount++;
		if (m_lCount > 999999l) {
			m_lCount = 1l;
		}
		return ("000000" + String.valueOf(m_lCount)).substring(String.valueOf(m_lCount).length());
	}
	
	private String getLegacyCeid(String sCeid) {
		if (sCeid != null) {
			if (sCeid.startsWith("BP")) {
				int i=2;
				while (i<sCeid.length()) {
					if (sCeid.charAt(i) != '0') {
						break;
					}
					i++;
				}
				try {
					sCeid = sCeid.substring(i).trim().toLowerCase();
				} catch (Exception e) {}
			}
		}
		return sCeid;
	}
	
	private void setLastIwTs(String sIwTs) {
		if ( (sIwTs != null) && ((m_sIwTs == null) || (m_sIwTs.compareTo(sIwTs) < 0)) ) {
			m_sIwTs = sIwTs;
		}
	}

	private void saveUsedSN() {
		if (m_mapOfUsedSN == null) {
			return;
		}
		String sCacheFile = m_mapOfArgs.get("CACHEFILE");
		if (sCacheFile.length()==0) {
			sCacheFile = USEDSNMAP_FILENAME;
		}

		String sKey = null;
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(sCacheFile));
			Iterator<String> iter = m_mapOfUsedSN.keySet().iterator();
			while (iter.hasNext()) {
				sKey = iter.next();
				bw.write(sKey);
				bw.write('\n');
				bw.write(m_mapOfUsedSN.get(sKey));
				bw.write('\n');
			}
			bw.flush();
		} catch (IOException ioe) {
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException ioe) {}
			}
		}

	}
	
	private void loadUsedSN() {
		m_mapOfUsedSN = new HashMap<String, String>();
		
		boolean isReset = m_mapOfArgs.containsKey("CACHERESET");
		
		String sCacheFile = m_mapOfArgs.get("CACHEFILE");
		if (sCacheFile.length()==0) {
			sCacheFile = USEDSNMAP_FILENAME;
		}
		File fMap = new File(sCacheFile);
		if (!fMap.exists()) {
			return;
		}
		if (isReset) {
			fMap.delete();
			return;
		}
		
		Calendar calBacklog = Calendar.getInstance(TimeZone.getTimeZone("Europe/Paris"));
		calBacklog.setTime(new java.util.Date());
		calBacklog.add(java.util.Calendar.MONTH, -3);
		String sCutOff = (new SimpleDateFormat("yyyyMMdd")).format(calBacklog.getTime());

		BufferedReader bufText = null;
		try {
			String sText = null;
			String sKey = null;
				bufText = new BufferedReader(new FileReader(fMap));
				while ((sText = bufText.readLine()) != null) {
					if (sKey == null) {
						sKey = sText;
						continue;
					}
					if ((sCutOff!=null) && (sCutOff.compareTo(sText)<0)) {
						m_mapOfUsedSN.put(new String(sKey), new String(sText));
					}
					sKey = null;
				}
		} catch (IOException ioe) {
		} finally {
			try {
				bufText.close();
			} catch (IOException e) {}
		}
	}
	

	private static List<String> getAsList(String sList) {
		List<String> listOfItems = new ArrayList<String>();
		if ((sList != null) && (sList.trim().length() > 0)) {
			String sToken = null;
			StringTokenizer sTokGrid = new StringTokenizer(sList, ",");
			while (sTokGrid.hasMoreTokens()) {
				sToken = sTokGrid.nextToken();
				if ((sToken != null) && (sToken.trim().length() > 0)) {
					listOfItems.add(sToken.trim());
				}
			}
			sTokGrid = null;
		}
		return listOfItems; 
	}

	/**
	 * Returns a statement to execute a query
	 * This method will attempt one time to reconnect to the db in case of SQLSTATE=08001 or 08003
	 * @return java.sql.Statement
	 * @throws Exception
	 */
	private Statement getDbStatement(boolean isScrollable) throws Exception {
		Statement s = null;
		if (m_dbConnection == null) {
			m_dbConnection = getIwDbConnection();
		}
		if (isScrollable) {
			s = m_dbConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		} else {
			s = m_dbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}
		return s;
	}

	private Connection getIwDbConnection() throws Exception {
		String sHost = m_mapOfArgs.get("DB.HOST");
		String sDbName = m_mapOfArgs.get("DB.NAME");
		String sDbPort = m_mapOfArgs.get("DB.PORT");
		if ((sDbPort != null) && (sDbPort.trim().length() > 0)) {
			sDbPort = ":" + sDbPort.trim();
		} else {
			sDbPort = "";
		}
		String sJdbcDriver = m_mapOfArgs.get("DB_DRIVERTYPE4");
		if ((sJdbcDriver == null) || (sJdbcDriver.trim().length() == 0)) {
			sJdbcDriver = "com.ibm.db2.jcc.DB2Driver";
		}
		return connectToDb(
				sJdbcDriver, 
				"jdbc:db2://" + sHost + sDbPort + "/" + sDbName
					+ ":retrieveMessagesFromServerOnGetMessage=true;emulateParameterMetaDataForZCalls=1;decimalStringFormat=2;decimalSeparator=1;timeFormat=1;dateFormat=1;timestampFormat=5;", 
				m_mapOfArgs.get("DB.USER"), 
				m_mapOfArgs.get("DB.PASSWORD"));
	}

	
	private Connection connectToDb(String sClassDriver, String sJdbcName,
			String sDbUser, String sDbPassword)
			throws Exception {

		Class.forName(sClassDriver).newInstance();

		Connection dbConnection = null;
		try {
			dbConnection = DriverManager.getConnection(sJdbcName, sDbUser, sDbPassword);
		} catch (SQLException sqle) {
			throw new Exception(formatExceptionMsg(sqle));
		}
		dbConnection.setAutoCommit(true);

		return dbConnection;
	}

	private String formatExceptionMsg(Throwable t) {
		StringBuilder errorBuffer = new StringBuilder();

		if (t instanceof SQLException) {
			SQLException sqle = (SQLException) t;
			while (sqle != null) {
				errorBuffer.append("[SQLState=");
				errorBuffer.append(sqle.getSQLState());
				errorBuffer.append("] Message=");
				errorBuffer.append(sqle.toString());
				sqle = sqle.getNextException();
				if (sqle != null) {
					errorBuffer.append(System.getProperty("line.separator"));
				}
			}
		} else {
			errorBuffer.append(t.getMessage());
		}

		return errorBuffer.toString();
	}

	private void closeDb() {
		if (m_dbConnection != null) {
			try {
				m_dbConnection.commit();
				m_dbConnection.close();
			} catch (Exception e) {}
		}
	}

	private void closeWriter() {
		if (m_Writer != null) {
			try {
				m_Writer.flush();
				m_Writer.close();
			} catch (Exception e) {}
		}
	}
	
	private void addRow(String[] arRow) throws IOException {
		if (m_Writer == null) {
			m_Writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("BHCNSI.xls"), "UTF-8")); // UTF-8//UTF-16LE
		}
		for (int k=0; k<arRow.length; k++) {
			if (arRow[k] != null) {
				m_Writer.write(cleanCSVspecialchars(arRow[k]));
			}
			if (k < (arRow.length-1)) {
				m_Writer.write(_csv_separator);
			}
		}
		m_Writer.write(_csv_endofline);
	}

	private String cleanCSVspecialchars(String sText) {
		if (sText == null) {
			return "";
		}
		if (sText.indexOf("\"") >= 0) { 
			StringBuilder sBuf = new StringBuilder(sText);
			for (int j=0; j<sBuf.length(); j++) {
				if (sBuf.charAt(j) == '"') {
					sBuf.insert(j, '"');
					j++;
				}
			}
			sText = sBuf.toString();
		}

		//sText = sText.replace('\r', ' ');
		//sText = sText.replace('\n', ' ');
		if ((sText.indexOf(";") >= 0) 
			|| (sText.indexOf(",") >= 0) 
			|| (sText.indexOf("\"") >= 0)) {
			sText = "\"" + sText + "\"";
		}
		return sText;
	}
	
}
