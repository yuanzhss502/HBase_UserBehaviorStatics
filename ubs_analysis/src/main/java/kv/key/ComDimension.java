package kv.key;

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComDimension extends BaseDimension {
    private ContactDimension contactDimension = new ContactDimension();
    private DateDimension dateDimension = new DateDimension();

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    @Override
    public int compareTo(Object o) {
        ComDimension anotherComDimension = (ComDimension) o;
        int result = this.dateDimension.compareTo(anotherComDimension.dateDimension);
        if (result != 0) return result;
        result = this.contactDimension.compareTo(anotherComDimension.contactDimension);
        return result;

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        contactDimension.write(dataOutput);
        dateDimension.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        contactDimension.readFields(dataInput);
        dateDimension.readFields(dataInput);

    }
}
