-- Custom TPC-H Query: Massive UNION across 300 subqueries
-- This query performs a UNION ALL operation with 300 separate scans of the lineitem table
-- Each subquery filters on a different l_orderkey range to simulate 300 different partitions

SELECT 1 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 0
UNION ALL
SELECT 2 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 1
UNION ALL
SELECT 3 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 2
UNION ALL
SELECT 4 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 3
UNION ALL
SELECT 5 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 4
UNION ALL
SELECT 6 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 5
UNION ALL
SELECT 7 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 6
UNION ALL
SELECT 8 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 7
UNION ALL
SELECT 9 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 8
UNION ALL
SELECT 10 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 9
UNION ALL
SELECT 11 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 10
UNION ALL
SELECT 12 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 11
UNION ALL
SELECT 13 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 12
UNION ALL
SELECT 14 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 13
UNION ALL
SELECT 15 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 14
UNION ALL
SELECT 16 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 15
UNION ALL
SELECT 17 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 16
UNION ALL
SELECT 18 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 17
UNION ALL
SELECT 19 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 18
UNION ALL
SELECT 20 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 19
UNION ALL
SELECT 21 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 20
UNION ALL
SELECT 22 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 21
UNION ALL
SELECT 23 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 22
UNION ALL
SELECT 24 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 23
UNION ALL
SELECT 25 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 24
UNION ALL
SELECT 26 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 25
UNION ALL
SELECT 27 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 26
UNION ALL
SELECT 28 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 27
UNION ALL
SELECT 29 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 28
UNION ALL
SELECT 30 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 29
UNION ALL
SELECT 31 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 30
UNION ALL
SELECT 32 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 31
UNION ALL
SELECT 33 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 32
UNION ALL
SELECT 34 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 33
UNION ALL
SELECT 35 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 34
UNION ALL
SELECT 36 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 35
UNION ALL
SELECT 37 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 36
UNION ALL
SELECT 38 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 37
UNION ALL
SELECT 39 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 38
UNION ALL
SELECT 40 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 39
UNION ALL
SELECT 41 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 40
UNION ALL
SELECT 42 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 41
UNION ALL
SELECT 43 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 42
UNION ALL
SELECT 44 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 43
UNION ALL
SELECT 45 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 44
UNION ALL
SELECT 46 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 45
UNION ALL
SELECT 47 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 46
UNION ALL
SELECT 48 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 47
UNION ALL
SELECT 49 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 48
UNION ALL
SELECT 50 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 49
UNION ALL
SELECT 51 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 50
UNION ALL
SELECT 52 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 51
UNION ALL
SELECT 53 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 52
UNION ALL
SELECT 54 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 53
UNION ALL
SELECT 55 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 54
UNION ALL
SELECT 56 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 55
UNION ALL
SELECT 57 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 56
UNION ALL
SELECT 58 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 57
UNION ALL
SELECT 59 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 58
UNION ALL
SELECT 60 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 59
UNION ALL
SELECT 61 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 60
UNION ALL
SELECT 62 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 61
UNION ALL
SELECT 63 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 62
UNION ALL
SELECT 64 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 63
UNION ALL
SELECT 65 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 64
UNION ALL
SELECT 66 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 65
UNION ALL
SELECT 67 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 66
UNION ALL
SELECT 68 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 67
UNION ALL
SELECT 69 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 68
UNION ALL
SELECT 70 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 69
UNION ALL
SELECT 71 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 70
UNION ALL
SELECT 72 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 71
UNION ALL
SELECT 73 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 72
UNION ALL
SELECT 74 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 73
UNION ALL
SELECT 75 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 74
UNION ALL
SELECT 76 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 75
UNION ALL
SELECT 77 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 76
UNION ALL
SELECT 78 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 77
UNION ALL
SELECT 79 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 78
UNION ALL
SELECT 80 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 79
UNION ALL
SELECT 81 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 80
UNION ALL
SELECT 82 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 81
UNION ALL
SELECT 83 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 82
UNION ALL
SELECT 84 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 83
UNION ALL
SELECT 85 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 84
UNION ALL
SELECT 86 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 85
UNION ALL
SELECT 87 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 86
UNION ALL
SELECT 88 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 87
UNION ALL
SELECT 89 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 88
UNION ALL
SELECT 90 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 89
UNION ALL
SELECT 91 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 90
UNION ALL
SELECT 92 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 91
UNION ALL
SELECT 93 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 92
UNION ALL
SELECT 94 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 93
UNION ALL
SELECT 95 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 94
UNION ALL
SELECT 96 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 95
UNION ALL
SELECT 97 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 96
UNION ALL
SELECT 98 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 97
UNION ALL
SELECT 99 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 98
UNION ALL
SELECT 100 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 99
UNION ALL
SELECT 101 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 100
UNION ALL
SELECT 102 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 101
UNION ALL
SELECT 103 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 102
UNION ALL
SELECT 104 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 103
UNION ALL
SELECT 105 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 104
UNION ALL
SELECT 106 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 105
UNION ALL
SELECT 107 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 106
UNION ALL
SELECT 108 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 107
UNION ALL
SELECT 109 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 108
UNION ALL
SELECT 110 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 109
UNION ALL
SELECT 111 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 110
UNION ALL
SELECT 112 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 111
UNION ALL
SELECT 113 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 112
UNION ALL
SELECT 114 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 113
UNION ALL
SELECT 115 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 114
UNION ALL
SELECT 116 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 115
UNION ALL
SELECT 117 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 116
UNION ALL
SELECT 118 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 117
UNION ALL
SELECT 119 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 118
UNION ALL
SELECT 120 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 119
UNION ALL
SELECT 121 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 120
UNION ALL
SELECT 122 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 121
UNION ALL
SELECT 123 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 122
UNION ALL
SELECT 124 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 123
UNION ALL
SELECT 125 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 124
UNION ALL
SELECT 126 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 125
UNION ALL
SELECT 127 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 126
UNION ALL
SELECT 128 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 127
UNION ALL
SELECT 129 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 128
UNION ALL
SELECT 130 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 129
UNION ALL
SELECT 131 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 130
UNION ALL
SELECT 132 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 131
UNION ALL
SELECT 133 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 132
UNION ALL
SELECT 134 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 133
UNION ALL
SELECT 135 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 134
UNION ALL
SELECT 136 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 135
UNION ALL
SELECT 137 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 136
UNION ALL
SELECT 138 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 137
UNION ALL
SELECT 139 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 138
UNION ALL
SELECT 140 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 139
UNION ALL
SELECT 141 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 140
UNION ALL
SELECT 142 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 141
UNION ALL
SELECT 143 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 142
UNION ALL
SELECT 144 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 143
UNION ALL
SELECT 145 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 144
UNION ALL
SELECT 146 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 145
UNION ALL
SELECT 147 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 146
UNION ALL
SELECT 148 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 147
UNION ALL
SELECT 149 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 148
UNION ALL
SELECT 150 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 149
UNION ALL
SELECT 151 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 150
UNION ALL
SELECT 152 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 151
UNION ALL
SELECT 153 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 152
UNION ALL
SELECT 154 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 153
UNION ALL
SELECT 155 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 154
UNION ALL
SELECT 156 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 155
UNION ALL
SELECT 157 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 156
UNION ALL
SELECT 158 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 157
UNION ALL
SELECT 159 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 158
UNION ALL
SELECT 160 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 159
UNION ALL
SELECT 161 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 160
UNION ALL
SELECT 162 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 161
UNION ALL
SELECT 163 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 162
UNION ALL
SELECT 164 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 163
UNION ALL
SELECT 165 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 164
UNION ALL
SELECT 166 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 165
UNION ALL
SELECT 167 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 166
UNION ALL
SELECT 168 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 167
UNION ALL
SELECT 169 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 168
UNION ALL
SELECT 170 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 169
UNION ALL
SELECT 171 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 170
UNION ALL
SELECT 172 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 171
UNION ALL
SELECT 173 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 172
UNION ALL
SELECT 174 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 173
UNION ALL
SELECT 175 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 174
UNION ALL
SELECT 176 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 175
UNION ALL
SELECT 177 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 176
UNION ALL
SELECT 178 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 177
UNION ALL
SELECT 179 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 178
UNION ALL
SELECT 180 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 179
UNION ALL
SELECT 181 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 180
UNION ALL
SELECT 182 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 181
UNION ALL
SELECT 183 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 182
UNION ALL
SELECT 184 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 183
UNION ALL
SELECT 185 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 184
UNION ALL
SELECT 186 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 185
UNION ALL
SELECT 187 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 186
UNION ALL
SELECT 188 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 187
UNION ALL
SELECT 189 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 188
UNION ALL
SELECT 190 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 189
UNION ALL
SELECT 191 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 190
UNION ALL
SELECT 192 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 191
UNION ALL
SELECT 193 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 192
UNION ALL
SELECT 194 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 193
UNION ALL
SELECT 195 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 194
UNION ALL
SELECT 196 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 195
UNION ALL
SELECT 197 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 196
UNION ALL
SELECT 198 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 197
UNION ALL
SELECT 199 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 198
UNION ALL
SELECT 200 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 199
UNION ALL
SELECT 201 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 200
UNION ALL
SELECT 202 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 201
UNION ALL
SELECT 203 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 202
UNION ALL
SELECT 204 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 203
UNION ALL
SELECT 205 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 204
UNION ALL
SELECT 206 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 205
UNION ALL
SELECT 207 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 206
UNION ALL
SELECT 208 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 207
UNION ALL
SELECT 209 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 208
UNION ALL
SELECT 210 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 209
UNION ALL
SELECT 211 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 210
UNION ALL
SELECT 212 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 211
UNION ALL
SELECT 213 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 212
UNION ALL
SELECT 214 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 213
UNION ALL
SELECT 215 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 214
UNION ALL
SELECT 216 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 215
UNION ALL
SELECT 217 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 216
UNION ALL
SELECT 218 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 217
UNION ALL
SELECT 219 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 218
UNION ALL
SELECT 220 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 219
UNION ALL
SELECT 221 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 220
UNION ALL
SELECT 222 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 221
UNION ALL
SELECT 223 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 222
UNION ALL
SELECT 224 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 223
UNION ALL
SELECT 225 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 224
UNION ALL
SELECT 226 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 225
UNION ALL
SELECT 227 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 226
UNION ALL
SELECT 228 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 227
UNION ALL
SELECT 229 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 228
UNION ALL
SELECT 230 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 229
UNION ALL
SELECT 231 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 230
UNION ALL
SELECT 232 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 231
UNION ALL
SELECT 233 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 232
UNION ALL
SELECT 234 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 233
UNION ALL
SELECT 235 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 234
UNION ALL
SELECT 236 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 235
UNION ALL
SELECT 237 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 236
UNION ALL
SELECT 238 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 237
UNION ALL
SELECT 239 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 238
UNION ALL
SELECT 240 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 239
UNION ALL
SELECT 241 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 240
UNION ALL
SELECT 242 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 241
UNION ALL
SELECT 243 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 242
UNION ALL
SELECT 244 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 243
UNION ALL
SELECT 245 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 244
UNION ALL
SELECT 246 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 245
UNION ALL
SELECT 247 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 246
UNION ALL
SELECT 248 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 247
UNION ALL
SELECT 249 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 248
UNION ALL
SELECT 250 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 249
UNION ALL
SELECT 251 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 250
UNION ALL
SELECT 252 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 251
UNION ALL
SELECT 253 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 252
UNION ALL
SELECT 254 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 253
UNION ALL
SELECT 255 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 254
UNION ALL
SELECT 256 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 255
UNION ALL
SELECT 257 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 256
UNION ALL
SELECT 258 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 257
UNION ALL
SELECT 259 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 258
UNION ALL
SELECT 260 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 259
UNION ALL
SELECT 261 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 260
UNION ALL
SELECT 262 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 261
UNION ALL
SELECT 263 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 262
UNION ALL
SELECT 264 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 263
UNION ALL
SELECT 265 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 264
UNION ALL
SELECT 266 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 265
UNION ALL
SELECT 267 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 266
UNION ALL
SELECT 268 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 267
UNION ALL
SELECT 269 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 268
UNION ALL
SELECT 270 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 269
UNION ALL
SELECT 271 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 270
UNION ALL
SELECT 272 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 271
UNION ALL
SELECT 273 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 272
UNION ALL
SELECT 274 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 273
UNION ALL
SELECT 275 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 274
UNION ALL
SELECT 276 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 275
UNION ALL
SELECT 277 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 276
UNION ALL
SELECT 278 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 277
UNION ALL
SELECT 279 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 278
UNION ALL
SELECT 280 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 279
UNION ALL
SELECT 281 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 280
UNION ALL
SELECT 282 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 281
UNION ALL
SELECT 283 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 282
UNION ALL
SELECT 284 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 283
UNION ALL
SELECT 285 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 284
UNION ALL
SELECT 286 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 285
UNION ALL
SELECT 287 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 286
UNION ALL
SELECT 288 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 287
UNION ALL
SELECT 289 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 288
UNION ALL
SELECT 290 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 289
UNION ALL
SELECT 291 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 290
UNION ALL
SELECT 292 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 291
UNION ALL
SELECT 293 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 292
UNION ALL
SELECT 294 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 293
UNION ALL
SELECT 295 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 294
UNION ALL
SELECT 296 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 295
UNION ALL
SELECT 297 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 296
UNION ALL
SELECT 298 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 297
UNION ALL
SELECT 299 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 298
UNION ALL
SELECT 300 as partition_id, count(*) as row_count FROM lineitem WHERE l_orderkey % 300 = 299
ORDER BY partition_id;
