package s.park.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 长这样：<br/>
 * <code>
 * {<br/>
 * &nbsp;&nbsp;"attackType": "infiltration",<br/>
 * &nbsp;&nbsp;"srcIp": "192.168.8.254",<br/>
 * &nbsp;&nbsp;"dstIp": "192.168.8.29",<br/>
 * &nbsp;&nbsp;"srcPort": 53,<br/>
 * &nbsp;&nbsp;"dstPort": 60653,<br/>
 * &nbsp;&nbsp;"protocol": "UDP",<br/>
 * &nbsp;&nbsp;"timestamp": 1689097244,<br/>
 * &nbsp;&nbsp;"uSec": 787534<br/>
 * }
 * </code>
 */
@Data
public class AttackMessage implements Serializable {
  @JsonProperty("attackType")
  private String attackType;
  @JsonProperty("srcIp")
  private String srcIp;
  @JsonProperty("dstIp")
  private String dstIp;
  @JsonProperty("protocol")
  private String protocol;
  @JsonProperty("srcPort")
  private Integer srcPort;
  @JsonProperty("dstPort")
  private Integer dstPort;
  @JsonProperty("uSec")
  private Long microsecond;
  @JsonProperty("timestamp")
  private Long timestamp;
  @JsonProperty("probRank")
  private List<KVP> scoreRank;
  @JsonProperty("value")
  private String value;
}
