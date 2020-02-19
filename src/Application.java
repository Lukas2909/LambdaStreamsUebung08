import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class Application implements IApplication {
    private ArrayList<String> areas;
    private ArrayList<String> shifts;
    private ArrayList<Record> records;

    private Predicate<Record> city10 = (Record record) -> record.getCity() == 10;
    private Predicate<Record> city40 = (Record record) -> record.getCity() == 40;

    private Predicate<Record> cityIn4050 = (Record record) -> ((record.getCity()==40)||(record.getCity()==50));
    private Predicate<Record> cityIn1020304050 = (Record record) -> ((record.getCity()==10)||(record.getCity()==20)||(record.getCity()==30)||(record.getCity()==40)||(record.getCity()==50));

    private Predicate<Record> cityNotIn1020 = (Record record) -> ((record.getCity()!=10)&&(record.getCity()!=20));
    private Predicate<Record> cityNotIn10203050 = (Record record) -> ((record.getCity()!=10)&&(record.getCity()!=20)&&(record.getCity()!=30)&&(record.getCity()!=50));
    private Predicate<Record> cityNotIn1020304050 = (Record record) -> ((record.getCity()!=10)&&(record.getCity()!=20)&&(record.getCity()!=30)&&(record.getCity()!=40)&&(record.getCity()!=50));

    private Predicate<Record> areaInAB = (Record record) -> ((record.getArea().equals("a"))||(record.getArea().equals("b")));
    private Predicate<Record> areaInAC = (Record record) -> ((record.getArea().equals("a"))||(record.getArea().equals("c")));
    private Predicate<Record> areaInBC = (Record record) -> ((record.getArea().equals("b"))||(record.getArea().equals("c")));

    private Predicate<Record> victimType1 = (Record record) -> record.getVictimType() == 1;
    private Predicate<Record> victimType2 = (Record record) -> record.getVictimType() == 2;

    private Comparator<Record> descDaysInHospitalComperator = (Record record1, Record record2)-> (int) (record2.getDaysInHospital() - record1.getDaysInHospital());

    public Application() {
        areas = new ArrayList<>();
        shifts = new ArrayList<>();
        records = new ArrayList<>();
    }

    public static void main(String... args) {
         Application application = new Application();
         application.initAreas();
         application.initShifts();
         application.importCSVFile(Configuration.instance.dataPath + "records.csv");

         System.out.println("Lambda-Streams Aufgabe 08 - Crime --- 8989700");
         System.out.println("");
         application.execute01();
         application.execute02();
         application.execute03();
         application.execute04();
         application.execute05();
         application.execute06();
         application.execute07();
         application.execute08();
         application.execute09();
         application.execute10();
         application.execute11();
         application.execute12();
         application.execute13();
         application.execute14();
    }

    public void execute01(){
        System.out.println("----- Abfrage 01: -----");
        System.out.println(records.stream().count());
        System.out.println();
    }

    public void execute02(){
        System.out.println("----- Abfrage 02: -----");
        System.out.println(records.stream().filter(city40).filter(x->x.getArea().equals("b")).filter(x->x.getVictimType()>=2).count());
        System.out.println();
    }

    public void execute03(){
        System.out.println("----- Abfrage 03: -----");
        System.out.print(records.stream().filter(city40).filter(areaInAC).filter(x->x.getVictimType()==3).filter(x->x.getVictimAge()<=18).count());
        System.out.println();
    }

    public void execute04(){
        System.out.println("----- Abfrage 04: -----");
        System.out.println(records.stream().filter((cityNotIn10203050)).filter(victimType1).filter(x->(x.getVictimAge()>=5)&&(x.getVictimAge()<=25)).count());
        System.out.println();
    }

    public void execute05(){
        System.out.println("----- Abfrage 05: -----");
        int sum=records.stream().filter(city10).filter(areaInAB).filter(x->((x.getVictimAge()>=20)&&(x.getVictimAge()<=25))).map(x->x.getDaysInHospital()).collect(Collectors.summingInt(Integer::intValue));
        System.out.println(sum);
        System.out.println();
    }

    public void execute06(){
        System.out.println("----- Abfrage 06: -----");
        System.out.println(records.stream().filter(cityNotIn10203050).filter(x->x.getVictimType()==1).filter(x-> ((x.getVictimAge()>=5)&&(x.getVictimAge()<=25))).collect(Collectors.averagingInt(x->x.getDaysInHospital())).intValue());
        System.out.println();
    }

    public void execute07(){
        System.out.println("----- Abfrage 07: -----");
        records.stream().filter(city40).filter(x->x.getArea().equals("b")).filter(x->((x.getVictimType()==1)||(x.getVictimType()==3))).filter(x->x.getVictimAge()==18).filter(x->x.getDaysInHospital()>=10).sorted(descDaysInHospitalComperator).map(x->x.getId()).limit(3).forEach(System.out :: println);
        System.out.println();
    }

    public void execute08(){
        System.out.println("----- Abfrage 08: -----");
        records.stream().filter(city10).filter(areaInAB).filter(x->x.getVictimType()==3).filter(x->x.getVictimAge()==18).filter(x->x.getDaysInHospital()>=10).sorted(Comparator.comparing(x->x.getArea())).sorted(descDaysInHospitalComperator).map(x->x.getId()).forEach(System.out :: println);
        System.out.println();
    }

    public void execute09(){
        System.out.println("----- Abfrage 09: -----");
        records.stream().collect(Collectors.groupingBy(Record::getShift, Collectors.counting())).forEach((k, v) -> System.out.println((k + " " + v)));
        System.out.println();
    }

    public void execute10(){
        System.out.println("----- Abfrage 10: -----");
        records.stream().filter(victimType2).filter(x->x.getVictimAge()==18).collect(Collectors.groupingBy(Record::getArea, Collectors.counting())).forEach((k, v) -> System.out.println((k + " " + v)));
        System.out.println();
    }

    public void execute11(){
        System.out.println("----- Abfrage 11: -----");
        records.stream().filter(cityIn1020304050).filter(x->x.getArea().equals("c")).filter(x->x.getVictimAge()>=60).collect(Collectors.groupingBy(Record::getVictimType, Collectors.counting())).forEach((k, v) -> System.out.println((k + " " + v)));
        System.out.println();
    }

    public void execute12(){
        System.out.println("----- Abfrage 12: -----");
        records.stream().filter(cityNotIn1020304050).filter(x->x.getShift().equals("n")).filter(x->x.getVictimAge()>=30).filter(x->x.getVictimAge()<=40).collect(Collectors.groupingBy(Record::getArea, Collectors.counting())).forEach((k, v) -> System.out.println((k + " " + v)));
        System.out.println();
    }

    public void execute13(){
        System.out.println("----- Abfrage 13: -----");
        records.stream().filter(cityNotIn1020).filter(areaInBC).filter(x->x.getShift().equals("d")).filter(x->x.getVictimAge()>=18).collect(Collectors.groupingBy(Record::getVictimType, Collectors.summingInt(x->x.getDaysInHospital()))).forEach((k, v) -> System.out.println((k + " " + v)));
        System.out.println();
    }

    public void execute14(){
        System.out.println("----- Abfrage 14: -----");
        records.stream().filter(cityIn4050).filter(areaInAC).filter(x->x.getShift().equals("n")).collect(Collectors.groupingBy(Record::getVictimType, Collectors.averagingInt(x->x.getVictimAge()))).forEach((k, v) -> System.out.println((k + " " + v.intValue())));
        System.out.println();
    }

    // 8: Reihenfolge nicht 100% identisch, da bei manchen Werten beide Parameter gleich sind und somit die Sortierung uneindeutig wird.. Sortierungsanforderung ist aber erfüllt
    // 11 - 14: Durch die zwischendurch erzeugte Map ist die Ausgabe direkt sortiert

    public void initAreas() {
        areas.add("a");
        areas.add("b");
        areas.add("c");
    }

    public void initShifts() {
        shifts.add("d");
        shifts.add("n");
    }

    public void importCSVFile(String fileName) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] strings = line.split(";");
                records.add(new Record(Integer.parseInt(strings[0]), Integer.parseInt(strings[1]), strings[2], strings[3], Integer.parseInt(strings[4]),
                        Integer.parseInt(strings[5]), Integer.parseInt(strings[6])));
            }
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }

    public void generateData() {
        for (int i = 0; i < Configuration.instance.maximumNumberOfRecords; i++) {
            int randomAreaIndex = Configuration.instance.randomNumberGenerator.nextInt(0, areas.size() - 1);
            int randomShiftIndex = Configuration.instance.randomNumberGenerator.nextInt(0, shifts.size() - 1);

            Record record = new Record(i + 1, Configuration.instance.randomNumberGenerator.nextInt(1, 100),
                    areas.get(randomAreaIndex), shifts.get(randomShiftIndex),
                    Configuration.instance.randomNumberGenerator.nextInt(1, 3),
                    Configuration.instance.randomNumberGenerator.nextInt(6, 60),
                    Configuration.instance.randomNumberGenerator.nextInt(1, 14));

            records.add(record);
        }
    }

    public void generateToCSVFile() {
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File(Configuration.instance.dataPath + "records.csv")));

            for (int i = 0; i < records.size(); i++)
                bufferedWriter.write(records.get(i).toString() + Configuration.instance.lineSeparator);

            bufferedWriter.close();
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }
}