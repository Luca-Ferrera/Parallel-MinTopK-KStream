package myapp;

public class User {
    private Long registertime;
    private String userid;
    private String regionid;
    private String gender;

    public Long getRegistertime() {
        return registertime;
    }

    public String getUserid() {
        return userid;
    }

    public String getRegionid() {
        return regionid;
    }

    public String getGender() {
        return gender;
    }

    public User(Long registertime, String userid, String regionid, String gender) {
        this.registertime = registertime;
        this.userid = userid;
        this.regionid = regionid;
        this.gender = gender;
    }

    public void setRegistertime(Long registertime) {
        this.registertime = registertime;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public void setRegionid(String regionid) {
        this.regionid = regionid;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }
}
