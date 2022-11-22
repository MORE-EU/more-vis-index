package eu.more2020.visual.domain;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvToBean;

import java.util.Objects;

/**
 * A Dataset.
 */
public class Sample extends CsvToBean {

    @CsvBindByName(column = "Continent")
    private String continent;

    @CsvBindByName(column = "Country")
    private String country;

    @CsvBindByName(column = "Area")
    private String area;

    @CsvBindByName(column = "City")
    private String city;

    @CsvBindByName(column = "Name")
    private String name;

    @CsvBindByName(column = "Latitude")
    private String lat;

    @CsvBindByName(column = "Longitude")
    private String lng;

    @CsvBindByName(column = "Manufacturer")
    private String manufacturer;

    @CsvBindByName(column = "Turbine")
    private String turbine;

    @CsvBindByName(column = "Hub height")
    private String hubHeight;

    @CsvBindByName(column = "Number of turbines")
    private String noOfTurbines;

    @CsvBindByName(column = "Total power")
    private String power;

    @CsvBindByName(column = "Developer")
    private String dev;

    @CsvBindByName(column = "Operator")
    private String operator;

    @CsvBindByName(column = "Owner")
    private String owner;

    @CsvBindByName(column = "Commissioning date")
    private String ComDate;


    public String getContinent() {
        return this.continent;
    }

    public void setContinent(String continent) {
        this.continent = continent;
    }

    public String getCountry() {
        return this.country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getArea() {
        return this.area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getCity() {
        return this.city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLat() {
        return this.lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLng() {
        return this.lng;
    }

    public void setLng(String lng) {
        this.lng = lng;
    }

    public String getManufacturer() {
        return this.manufacturer;
    }

    public void setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
    }

    public String getTurbine() {
        return this.turbine;
    }

    public void setTurbine(String turbine) {
        this.turbine = turbine;
    }

    public String getHubHeight() {
        return this.hubHeight;
    }

    public void setHubHeight(String hubHeight) {
        this.hubHeight = hubHeight;
    }

    public String getNoOfTurbines() {
        return this.noOfTurbines;
    }

    public void setNoOfTurbines(String noOfTurbines) {
        this.noOfTurbines = noOfTurbines;
    }

    public String getPower() {
        return this.power;
    }

    public void setPower(String power) {
        this.power = power;
    }

    public String getDev() {
        return this.dev;
    }

    public void setDev(String dev) {
        this.dev = dev;
    }

    public String getOperator() {
        return this.operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getOwner() {
        return this.owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getComDate() {
        return this.ComDate;
    }

    public void setComDate(String ComDate) {
        this.ComDate = ComDate;
    }


    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Sample)) {
            return false;
        }
        Sample sample = (Sample) o;
        return Objects.equals(continent, sample.continent) && Objects.equals(country, sample.country) && Objects.equals(area, sample.area) && Objects.equals(city, sample.city) && Objects.equals(name, sample.name) && Objects.equals(lat, sample.lat) && Objects.equals(lng, sample.lng) && Objects.equals(manufacturer, sample.manufacturer) && Objects.equals(turbine, sample.turbine) && Objects.equals(hubHeight, sample.hubHeight) && Objects.equals(noOfTurbines, sample.noOfTurbines) && Objects.equals(power, sample.power) && Objects.equals(dev, sample.dev) && Objects.equals(operator, sample.operator) && Objects.equals(owner, sample.owner) && Objects.equals(ComDate, sample.ComDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(continent, country, area, city, name, lat, lng, manufacturer, turbine, hubHeight, noOfTurbines, power, dev, operator, owner, ComDate);
    }

}

