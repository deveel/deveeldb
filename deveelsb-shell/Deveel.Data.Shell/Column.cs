using System;

namespace Deveel.Data.Shell {
	/**
 * Represents the meta data for a telational table Column
 *
 * @author Martin Grotzke
 */
public sealed class Column : IComparable {
     private String _name;
     private int _position; // starting at 1
     private String _type;
     private int _size;
     private bool _nullable;
     private String _default;
     private ColumnPkInfo _pkInfo;
     private ColumnFkInfo _fkInfo;
     
     public Column (String name) {
         _name = name;
     }

    public String getName() {
        return _name;
    }

    public void setName(String s) {
        _name = s;
    }

    public String getDefault() {
        return _default;
    }

    public String getType() {
        return _type;
    }

    /**
     * Set the default value for this Column.
     * @param defaultValue
     */
    public void setDefault(String defaultValue) {
        _default = defaultValue;
    }

    public void setType(String s) {
        _type = s;
    }

    public int getSize() {
        return _size;
    }

    public void setSize(int i) {
        _size = i;
    }

    public bool isNullable() {
        return _nullable;
    }

    public void setNullable(bool b) {
        _nullable = b;
    }

    public int getPosition() {
        return _position;
    }

    public void setPosition(int i) {
        _position = i;
    }

    public bool isPartOfPk() {
        return _pkInfo != null;
    }
    
    public ColumnPkInfo getPkInfo() {
        return _pkInfo;
    }
    
    public void setPkInfo(ColumnPkInfo pkInfo) {
        _pkInfo = pkInfo;
    }
    
    public bool isForeignKey() {
        return _fkInfo != null;
    }

    public ColumnFkInfo getFkInfo() {
        return _fkInfo;
    }

    public void setFkInfo(ColumnFkInfo info) {
        _fkInfo = info;
    }

    /* 
     * Compares both <code>Column</code>s according to their position.
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int CompareTo(Object o) {
        int result = 1;
        Column other = (Column)o;
        if ( other.getPosition() < _position )
            result = -1;
        else if ( other.getPosition() == _position )
            result = 0;
        return result;
    }
    
    /**
     * 
     * @param o
     * @param colNameIgnoreCase  specifies if column names shall be compared in a case insensitive way.
     * @return if the columns are equal
     */
    public bool Equals(Object o, bool colNameIgnoreCase) {
        if (o is Column) {
            Column other = (Column)o;
            
            if (_size != other._size)
                return false;
            
            // ignore the position, it's not important
            /*
            if (_position != other._position)
                return false;
            */
                
            if (_nullable != other._nullable)
                return false;
            
            if ( ( _name == null && other._name != null )
               || ( _name != null 
                    && ( colNameIgnoreCase && String.Compare(_name, other._name, true) != 0
					|| !colNameIgnoreCase && !_name.Equals(other._name)
                          )
                  )
               )
               return false;
            
            if ( ( _type == null && other._type !=null )
               || ( _type != null && !_type.Equals(other._type) ) )
              return false;
              
            if ( ( _default == null && other._default !=null )
               || ( _default != null && !_default.Equals(other._default) ) )
              return false;
              
            if ( ( _pkInfo == null && other._pkInfo !=null )
               || ( _pkInfo != null && !_pkInfo.Equals(other._pkInfo) ) )
              return false;
              
            if ( ( _fkInfo == null && other._fkInfo !=null )
               || ( _fkInfo != null && !_fkInfo.Equals(other._fkInfo) ) )
              return false;
              
        }
        return true;
    }

}
}