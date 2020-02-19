public class Record {
    private int id;
    private int city;
    private String area;
    private String shift;
    private int victimType;
    private int victimAge;
    private int daysInHospital;

    public Record(int id, int city, String area, String shift, int victimType, int victimAge, int daysInHospital) {
        this.id = id;
        this.city = city;
        this.area = area;
        this.shift = shift;
        this.victimType = victimType;
        this.victimAge = victimAge;
        this.daysInHospital = daysInHospital;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(id).append(";").append(city).append(";").append(area).append(";");
        stringBuilder.append(shift).append(";").append(victimType).append(";").append(victimAge).append(";");
        stringBuilder.append(daysInHospital);
        return stringBuilder.toString();
    }

    public int getId() {
        return id;
    }

    public int getCity() {
        return city;
    }

    public String getArea() {
        return area;
    }

    public String getShift() {
        return shift;
    }

    public int getVictimType() {
        return victimType;
    }

    public int getVictimAge() {
        return victimAge;
    }

    public int getDaysInHospital() {
        return daysInHospital;
    }
}