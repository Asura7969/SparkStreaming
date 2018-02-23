import scala.math.Ordered;

import java.io.Serializable;

/**
 * Created by gongwenzhou on 2018/2/23.
 */
public class ScondarySortKey implements Ordered<ScondarySortKey>,Serializable{

    private int first;
    private int second;

    @Override
    public int compare(ScondarySortKey that) {
        if(this.first != that.first){
            return this.second - that.second;
        }else{
            return this.first - that.first;
        }
    }

    @Override
    public boolean $less(ScondarySortKey that) {
        if(this.first < that.first){
            return true;
        }else if(this.first == that.first && this.second < that.second){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(ScondarySortKey that) {
        if(this.first > that.first){
            return true;
        }else if(this.first == that.first && this.second > that.second){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(ScondarySortKey that) {
        if(this.$less(that)){
            return true;
        }else if(this.first == that.first && this.second == that.second){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(ScondarySortKey that) {
        if(this.$greater(that)){
            return true;
        }else if(this.first == that.first && this.second == that.second){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(ScondarySortKey that) {
        if(this.first != that.first){
            return this.second - that.second;
        }else{
            return this.first - that.first;
        }
    }


    public ScondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
