package s.park.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class KVP implements Serializable {
  String label;
  Float score;
}
