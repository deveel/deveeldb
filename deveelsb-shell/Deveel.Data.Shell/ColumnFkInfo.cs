using System;
using System.Text;

namespace Deveel.Data.Shell {
	public sealed class ColumnFkInfo {
        
    private String _fkName;
    private String _pkTable;
    private String _pkColumn;
        
    public ColumnFkInfo(String fkName, String pkTable, String pkColumn) {
        _fkName = fkName;
        _pkTable = pkTable;
        _pkColumn = pkColumn;
    }

    /**
     * @return the name of the foreign key.
     */
    public String getFkName() {
        return _fkName;
    }
    
    public override bool Equals (Object other) {
        if (other != null && other is ColumnFkInfo) {
            ColumnFkInfo o = (ColumnFkInfo)other;
            if ( _fkName != null && !_fkName.Equals(o.getFkName())
                 || _fkName == null && o.getFkName() != null ) {
                return false;
            }
            else if ( _pkTable != null && !_pkTable.Equals(o.getPkTable())
                      || _pkTable == null && o.getPkTable() != null ) {
                return false;
            }
            else if ( _pkColumn != null && !_pkColumn.Equals(o.getPkColumn())
                      || _pkColumn == null && o.getPkColumn() != null ) {
                return false;
            }
            else {
                return true;
            }
        }
        return false;
    }

    /**
     * @return the primary key colum name (should this return a Column?)
     */
    public String getPkColumn() {
        return _pkColumn;
    }

    /**
     * @return the name of the primary key table (should this return a Table?)
     */
    public String getPkTable() {
        return _pkTable;
    }
    
    public override String ToString() {
        StringBuilder sb = new StringBuilder("ColumnFkInfo [" );
        sb.Append( "fkName: " ).Append( _fkName );
        sb.Append( ", pkTable: " ).Append( _pkTable );
        sb.Append( ", pkColumn: " ).Append( _pkColumn );
        sb.Append( "]" );
        return sb.ToString();
    }

}
}