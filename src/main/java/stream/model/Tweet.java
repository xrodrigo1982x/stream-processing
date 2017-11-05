package stream.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Tweet {

    private String id;
    private String user;
    private String text;
    private Date date;

    private List<String> quotes;
    private List<String> hashTags;

}
