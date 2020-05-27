package kv.key;

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ContactDimension extends BaseDimension {
    private String id;
    private String name;
    private String telephone;

    public ContactDimension() {
        super();
    }

    public ContactDimension(String telephone, String name) {

        super();

        this.name = name;
        this.telephone = telephone;

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContactDimension that = (ContactDimension) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(telephone, that.telephone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, telephone);
    }

    @Override
    public int compareTo(Object o) {
        ContactDimension anotherContactDimension = (ContactDimension) o;
        int result = this.name.compareTo(anotherContactDimension.name);
        if (result != 0) return result;

        result = this.telephone.compareTo(anotherContactDimension.telephone);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeUTF(this.name);
        dataOutput.writeUTF(this.telephone);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.id = dataInput.readUTF();
        this.telephone = dataInput.readUTF();
        this.name = dataInput.readUTF();

    }
}
