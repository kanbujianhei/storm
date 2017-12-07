package com.yijiupi.himalaya.pojo;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

public class RowMap implements Serializable {

	private static final long serialVersionUID = 305257130104070035L;

	private boolean commit;

	private LinkedHashMap<String, Object> data;

	private String database;

	private LinkedHashMap<String, Object> old;

	private List<String> pkColumns;

	private String position;

	private Long server_id;

	private String table;

	private Long thread_id;

	private Long ts;

	private String type;

	private Long xid;

	public LinkedHashMap<String, Object> getData() {
		return data;
	}

	public Object getData(String key) {
		return this.data.get(key);
	}

	public String getDatabase() {
		return database;
	}

	public LinkedHashMap<String, Object> getOld() {
		return old;
	}

	public List<String> getPkColumns() {
		return pkColumns;
	}

	public String getPosition() {
		return position;
	}

	public Long getServer_id() {
		return server_id;
	}

	public String getTable() {
		return table;
	}

	public Long getThread_id() {
		return thread_id;
	}

	public Long getTs() {
		return ts;
	}

	public String getType() {
		return this.type;
	}

	public Long getXid() {
		return xid;
	}

	public boolean isCommit() {
		return this.commit;
	}

	public void setcommit() {
		this.commit = true;
	}

	public void setCommit(boolean commit) {
		this.commit = commit;
	}

	public void setData(LinkedHashMap<String, Object> data) {
		this.data = data;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public void setOld(LinkedHashMap<String, Object> old) {
		this.old = old;
	}

	public void setPkColumns(List<String> pkColumns) {
		this.pkColumns = pkColumns;
	}

	public void setPosition(String position) {
		this.position = position;
	}

	public void setServer_id(Long server_id) {
		this.server_id = server_id;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setThread_id(Long thread_id) {
		this.thread_id = thread_id;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setXid(Long xid) {
		this.xid = xid;
	}

	@Override
	public String toString() {
		return "RowMap [ commit=" + commit + ", data=" + data + ", database=" + database + ", old=" + old
				+ ", pkColumns=" + pkColumns + ", position=" + position + ", server_id=" + server_id + ", table="
				+ table + ", thread_id=" + thread_id + ", ts=" + ts + ", type=" + type + ", xid=" + xid + "]";
	}

}
