package eu.more2020.visual.index.domain;

import eu.more2020.visual.index.domain.Dataset.AbstractDataset;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * A Dataset.
 */
public class Farm implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotNull
    private String name;

    @NotNull
    private Integer type;

    private ArrayList<AbstractDataset> data;

    public String getName() {
        return name;
    }

    public Integer getType() {
        return type;
    }

    public ArrayList<AbstractDataset> getData() {
        return data;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public void setData(ArrayList<AbstractDataset> data) {
        this.data = data;
    }


}
