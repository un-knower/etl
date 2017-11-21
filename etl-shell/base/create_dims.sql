-- 渠道维度表
CREATE TABLE IF NOT EXISTS base_dw.dim_channel (
  channel_id VARCHAR(50) COMMENT '渠道编号', 
  channel_name VARCHAR(255) COMMENT '渠道名称',
  status TINYINT(4) COMMENT '状态',
  PRIMARY KEY (channel_id)
) COMMENT '渠道维度表';