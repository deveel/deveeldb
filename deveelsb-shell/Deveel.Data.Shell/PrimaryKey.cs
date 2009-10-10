using System;
using System.Collections;

namespace Deveel.Data.Shell {
	public sealed class PrimaryKey {
    
    public const int INVALID_INDEX = -1;
    
    private String _name;
    private IDictionary /*<String, ColumnPkInfo>*/ _columns;  // column name -> pk info specific to column
    
    public PrimaryKey() {
        _columns = new Hashtable();
    }
    
    public void addColumn(String columnName, String columnPkName, int columnPkIndex) {
        _columns.Add(columnName, new ColumnPkInfo(columnPkName, columnPkIndex));
    }
    
    public bool columnParticipates(String column) {
        return _columns.Contains(column);
    }
    
    /*
    public int getColumnIndex(String column) {
        int result = INVALID_INDEX;
        ColumnPkInfo info = (ColumnPkInfo)_columns.get(column);
        if (info != null)
            result = info.getColumnIndex();
        return result;
    }
    */
    
    public ColumnPkInfo getColumnPkInfo(String column) {
        return (ColumnPkInfo)_columns[column];
    }
    
    public IDictionary getColumns() {
        return _columns;
    }

    public String getName() {
        return _name;
    }

    public void setName(String s) {
        _name = s;
    }
    
    public override bool Equals(Object other) {
        if (other != null && other is PrimaryKey) {
            PrimaryKey o = (PrimaryKey)other;
            if ( _name != null && !_name.Equals(o.getName())
                || _name == null && o.getName() != null )
                return false;

            if ( _columns != null && !_columns.Equals(o.getColumns())
                || _columns == null && o.getColumns() != null )
                return false;
            
            return true;
        }
        return false;
    }

}
}